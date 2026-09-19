#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# Bootstrap del backend remoto de Terraform (equivalente a lo que
# pedía la Pre-entrega 1). Se corre UNA sola vez, a mano, antes de
# terraform init — el backend no se puede crear con Terraform
# apuntando a sí mismo.
#
# Uso:
#   ./bootstrap-backend.sh <nombre-bucket-state> [--legacy-dynamodb <nombre-tabla>]
#
# Por defecto crea SOLO el bucket S3, para usar con use_lockfile
# (locking nativo de S3, Terraform >= 1.10). Si tu Terraform es
# anterior a 1.10, pasá --legacy-dynamodb <nombre-tabla> para que
# además cree la tabla DynamoDB de locking.
# ─────────────────────────────────────────────────────────────────
set -e

REGION="${AWS_REGION:-us-east-1}"
BUCKET_NAME="${1:?Uso: ./bootstrap-backend.sh <nombre-bucket-state> [--legacy-dynamodb <nombre-tabla>]}"
DYNAMO_TABLE=""

if [ "$2" = "--legacy-dynamodb" ]; then
  DYNAMO_TABLE="${3:?Falta el nombre de la tabla después de --legacy-dynamodb}"
fi

echo "Creando bucket de state: $BUCKET_NAME en $REGION..."
if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION"
else
  aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"
fi

aws s3api put-bucket-versioning --bucket "$BUCKET_NAME" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption --bucket "$BUCKET_NAME" \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3api put-public-access-block --bucket "$BUCKET_NAME" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

if [ -n "$DYNAMO_TABLE" ]; then
  echo "Creando tabla DynamoDB de locking (modo legacy): $DYNAMO_TABLE..."
  aws dynamodb create-table \
    --table-name "$DYNAMO_TABLE" \
    --attribute-definitions AttributeName=LockID,AttributeType=S \
    --key-schema AttributeName=LockID,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --region "$REGION"
fi

echo ""
echo "Listo. Ahora completá environments/dev/main.tf con:"
echo "  bucket = \"$BUCKET_NAME\""
echo "  region = \"$REGION\""
if [ -n "$DYNAMO_TABLE" ]; then
  echo "  dynamodb_table = \"$DYNAMO_TABLE\"   (en vez de use_lockfile = true)"
else
  echo "  use_lockfile = true"
fi
