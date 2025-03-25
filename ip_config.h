#define LOCALHOST "0.0.0.0"
#define QUERY_ENGINE_PORT "40100" 

#define STORAGE_ENGINE_IP (getenv("STORAGE_ENGINE_DNS") ? getenv("STORAGE_ENGINE_DNS") : "0.0.0.0")
#define SE_INTERFACE_PORT "40200"

#define INSTANCE_NAME (getenv("INSTANCE_NAME") ? getenv("INSTANCE_NAME") : "")

#define INSTANCE_TYPE (getenv("INSTANCE_TYPE") ? getenv("INSTANCE_TYPE") : "local")