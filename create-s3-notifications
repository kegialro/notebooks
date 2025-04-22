import json
import boto3
import urllib.parse
import urllib3
import logging

# Configuración del cliente S3 y del logger
s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def send_response(event, context, response_status, reason=None):
    """Envía una respuesta a CloudFormation con el estado de la operación."""
    response_body = json.dumps({
        'Status': response_status,
        'Reason': reason or f'See the details in CloudWatch Log Stream: {context.log_stream_name}',
        'PhysicalResourceId': event.get('PhysicalResourceId', event['LogicalResourceId']),
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'NoEcho': False,
    })
    
    response_url = event['ResponseURL']
    headers = {
        'Content-Type': '',
        'Content-Length': str(len(response_body))
    }
    
    http = urllib3.PoolManager()
    try:
        response = http.request('PUT', response_url, body=response_body, headers=headers)
        logger.info(f"Response sent: {response.status}, {response.reason}")
        return response.status, response.reason
    except Exception as e:
        logger.error(f"Failed to send response: {str(e)}")
        raise

def merge_configurations(request_type, input_config, current_config):
    """Fusiona la configuración de notificaciones del bucket S3 con la configuración actual."""
    valid_keys = {'TopicConfigurations', 'QueueConfigurations', 'LambdaFunctionConfigurations', 'EventBridgeConfiguration'}
    merged_config = {key: value for key, value in current_config.items() if key in valid_keys}
    
    for key, value in input_config.items():
        if key in valid_keys:
            input_ids = {obj['Id'] for obj in value}
            if key in current_config:
                current_value = current_config[key]
                if request_type == 'Delete':
                    merged_config[key] = [obj for obj in current_value if obj['Id'] not in input_ids]
                else:
                    filter_config = [obj for obj in current_value if obj['Id'] not in input_ids]
                    merged_config[key] = filter_config + value
            else:
                merged_config[key] = value
                
    return merged_config

def log(obj):
    """Loguea información de manera estructurada."""
    logger.info(json.dumps(obj, indent=2))

def log_error(obj):
    """Loguea errores de manera estructurada."""
    logger.error(json.dumps(obj, indent=2))

def lambda_handler(event, context):
    """Función principal que maneja la configuración de notificaciones del bucket S3."""
    logger.info("Received event: " + json.dumps(event, indent=2))
    props = event['ResourceProperties']
    bucket_name = props['BucketName']
    
    try:
        # Obtener la configuración actual del bucket S3
        current_configuration = s3.get_bucket_notification_configuration(Bucket=bucket_name)
        
        # Fusionar la nueva configuración con la existente
        merged_configuration = merge_configurations(event['RequestType'], props['NotificationConfiguration'], current_configuration)
        
        # Preparar los parámetros para actualizar la configuración del bucket
        put_params = {
            'Bucket': bucket_name,
            'NotificationConfiguration': merged_configuration
        }
        
        # Loguear la configuración previa y la nueva
        log({
            'bucket': bucket_name,
            'previousConfiguration': current_configuration,
            'newConfiguration': merged_configuration
        })
        
        # Aplicar la nueva configuración al bucket
        s3.put_bucket_notification_configuration(**put_params)
        
        # Enviar respuesta de éxito a CloudFormation
        send_response(event, context, 'SUCCESS')
    except Exception as e:
        # Loguear el error y enviar respuesta de fallo a CloudFormation
        log_error(f"Error processing event: {str(e)}")
        send_response(event, context, 'FAILED', str(e))

