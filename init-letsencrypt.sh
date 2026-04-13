#!/bin/bash

domains=("localhost") 
email="dataspecialist@abellioproductions.ie" 
rsa_key_size=4096
data_path="./certbot/conf"
staging=0 

if ! [ -x "$(command -v docker-compose)" ]; then
  echo 'Error: docker-compose is not installed.' >&2
  exit 1
fi

if [ -d "$data_path" ]; then
  read -p "Exist directory $data_path. Rewrite certificates? (y/N) " decision
  if [ "$decision" != "Y" ] && [ "$decision" != "y" ]; then
    exit
  fi
fi

echo 
path="/etc/letsencrypt/live/$domains"
mkdir -p "$data_path/live/$domains"
docker-compose run --rm --entrypoint "\
  openssl req -x509 -nodes -newkey rsa:$rsa_key_size -days 1 \
    -keyout '$path/privkey.pem' \
    -out '$path/fullchain.pem' \
    -subj '/CN=localhost'" certbot

echo 
docker-compose up --force-recreate -d nginx

echo 
if [ "$domains" = "localhost" ]; then
  echo "Generating self-signed certificate for localhost..."
  docker-compose run --rm --entrypoint "openssl req -x509 -nodes -newkey rsa:$rsa_key_size -days 365 \
    -keyout '$path/privkey.pem' \
    -out '$path/fullchain.pem' \
    -subj '/CN=localhost'" certbot
  
  echo "Reloading Nginx..."
  docker-compose exec nginx nginx -s reload
  exit 0
fi

docker-compose run --rm --entrypoint "\
  rm -Rf /etc/letsencrypt/live/$domains && \
  rm -Rf /etc/letsencrypt/archive/$domains && \
  rm -Rf /etc/letsencrypt/renewal/$domains.conf" certbot

echo
domain_args=""
for domain in "${domains[@]}"; do
  domain_args="$domain_args -d $domain"
done

case "$email" in
  "") email_arg="--register-unsafely-without-email" ;;
  *) email_arg="--email $email" ;;
esac

if [ $staging != "0" ]; then staging_arg="--staging"; fi

docker-compose run --rm --entrypoint "\
  certbot certonly --webroot -w /var/www/certbot \
    $staging_arg \
    $email_arg \
    $domain_args \
    --rsa-key-size $rsa_key_size \
    --agree-tos \
    --force-renewal" certbot

echo
docker-compose exec nginx nginx -s reload
echo