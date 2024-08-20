person_value_v1 = """
{
    "namespace": "com.udemy.kafka.course",
    "name": "Person",
    "type": "record",
    "fields": [
        {
            "name": "name", 
            "type": "string"
        },
        {
            "name": "title", 
            "type": "string"
        }
    ]
}
"""


person_value_v2 = """
{
    "namespace": "com.udemy.kafka.course",
    "name": "Person",
    "type": "record",
    "fields": [
        {
            "name": "first_name", 
            "type": ["null", "string"],
            "default": null
        },
        {
            "name": "last_name", 
            "type": ["null", "string"],
            "default": null
        },
        {
            "name": "title", 
            "type": "string"
        }
    ]
}
"""
