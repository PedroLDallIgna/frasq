from typing import Any, Tuple


def serialize_payload(data: Any) -> Tuple[bytes, str]:
    """
    Serializes the given data into bytes and determines the appropriate content type.
    
    Args:
        data (Any): The data to be serialized.
    Returns:
        Tuple[bytes, str]: A tuple containing the serialized data as bytes and the content type as a string.
    Raises:
        ValueError: If the data is None.    
    """
    
    if data is None:
        raise ValueError("Cannot serialize None data")
    
    if hasattr(data, 'model_dump_json'):
        return data.model_dump_json().encode('utf-8'), 'application/json'
    elif hasattr(data, 'json') and callable(getattr(data, 'json')):
        return data.json().encode('utf-8'), 'application/json'
    
    if hasattr(data, 'SerializeToString') and callable(getattr(data, 'SerializeToString')):
        return data.SerializeToString(), 'application/x-protobuf'
    
    if isinstance(data, (dict, list)):
        import json
        return json.dumps(data, ensure_ascii=False).encode('utf-8'), 'application/json'

    if isinstance(data, bytes):
        return data, 'application/octet-stream'
    
    return str(data).encode('utf-8'), 'text/plain'
