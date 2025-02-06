import os
import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    for record in event.get('Records', []):
        try:
            body = json.loads(record.get('body', '{}'))

            # Verifica que el mensaje contenga un evento válido de S3
            if not body or 'Records' not in body or not body['Records']:
                print(f"Mensaje sin eventos de S3: {body}")
                continue
            
            # Extraer bucket y key del mensaje
            s3_event = body['Records'][0]
            bucket = s3_event['s3']['bucket']['name']
            key = s3_event['s3']['object']['key']

            print(f"Procesando archivo: s3://{bucket}/{key}")

            # Definir la nueva ruta en "failed-documents/"
            failed_key = f"failed-documents/{key}"

            # Copiar archivo a la nueva ubicación
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=failed_key
            )
            print(f"Copiado a: s3://{bucket}/{failed_key}")

            # Eliminar el archivo original
            s3_client.delete_object(Bucket=bucket, Key=key)
            print(f"Eliminado original: s3://{bucket}/{key}")

        except Exception as e:
            print(f"Error procesando mensaje: {record}")
            print(f"Detalle del error: {str(e)}")

    return {"statusCode": 200}
