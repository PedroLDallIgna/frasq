from typing import Type, TypeVar

T = TypeVar('T')

def deserialize_payload(data: bytes, content_type: str | None, schema: Type[T] | None = None) -> T:

    if not data:
        return None
    
    if content_type == 'application/x-protobuf':
        if schema and hasattr(schema, 'ParseFromString') and callable(getattr(schema, 'ParseFromString')):
            try:
                msg = schema()
                msg.ParseFromString(data)
                return msg
            except Exception as e:
                raise ValueError(f"Failed to deserialize Protobuf: {e}")
        else:
            raise ValueError("Schema must be provided for Protobuf deserialization")
    
    elif content_type == 'application/json':
        if schema and hasattr(schema, 'model_validate_json'):
            return schema.model_validate_json(data.decode('utf-8'))
        
        if schema is dict:
            import json
            try:
                return json.loads(data.decode('utf-8'))
            except json.JSONDecodeError as e:
                raise ValueError(f"Failed to decode JSON: {e}")
            
    elif content_type == 'application/octet-stream':
        return data
    
    else:
        try:
            return data.decode('utf-8')
        except UnicodeDecodeError as e:
            return data
