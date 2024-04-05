import kopf
import kubernetes
from kubernetes import client, config
from kubernetes.client.models import V1ObjectMeta, V1RoleBinding, V1RoleRef
from kubernetes.client import CustomObjectsApi

import re
import yaml
import logging
import datetime
from datetime import datetime, timedelta, timezone

FINALIZER_NAME = "iamkube.local/finalizer"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load our cluster configurations
config.load_kube_config()

# API clients for Core, RBAC, CustomObjectsApi and ConfigMap resources
api_instance = client.CoreV1Api()
rbac_api_instance = client.RbacAuthorizationV1Api()
custom_api = CustomObjectsApi()



def serviceaccount_name(username, metadata_name):
    serviceaccount_name_combined = f"{username}-{metadata_name}".lower()
    compliant_name = re.sub(r'[^a-z0-9-]', '-', serviceaccount_name_combined)
    return compliant_name[:253]

def create_secret(sa_name, expiry_days, target_namespace):
    secret_name = f"{sa_name}-secret"
    expiry_time = datetime.utcnow() + timedelta(days=expiry_days)
    secret_body = client.V1Secret(
        metadata=client.V1ObjectMeta(name=secret_name, annotations={
            "kubernetes.io/service-account.name": sa_name,
            "expiration": expiry_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        }),
        type="kubernetes.io/service-account-token"
    )
    api_instance.create_namespaced_secret(namespace=target_namespace, body=secret_body)
    return secret_name

@kopf.on.create('iamkube.local', 'v1', 'iamkubes')
def create_iamkube(spec, name, meta, **kwargs):
    username = spec.get('username')
    target_namespace = spec.get('namespace', 'default')
    expiry_days = int(spec.get('expiry', 0))
    role_name = spec.get('role', None)
    sa_name = serviceaccount_name(username, name)

    # Check if the ServiceAccount already exists
    try:
        api_instance.read_namespaced_service_account(sa_name, namespace=target_namespace)
        logging.info(f"ServiceAccount {sa_name} already exists. Skipping creation.")
    except kubernetes.client.rest.ApiException as e:
        if e.status != 404:
            logging.error(f"Error occurred while checking ServiceAccount existence: {e}")
            return

    # Create ServiceAccount
    sa_body = client.V1ServiceAccount(metadata=client.V1ObjectMeta(name=sa_name))
    try:
        api_instance.create_namespaced_service_account(namespace=target_namespace, body=sa_body)
    except kubernetes.client.rest.ApiException as e:
        if e.status != 409:  # Conflict
            logging.error(f"Error occurred while creating ServiceAccount: {e}")
            return
        else:
            logging.info(f"ServiceAccount {sa_name} already exists. Skipping creation.")

    # Create Secret
    secret_name = create_secret(sa_name, expiry_days, target_namespace)

    if role_name:
        # Create RoleBinding
        rb_name = f"{sa_name}-rb"
        rb_body = V1RoleBinding(
            metadata=V1ObjectMeta(name=rb_name, namespace=target_namespace),
            subjects=[{
                "kind": "ServiceAccount",
                "name": sa_name,
                "namespace": target_namespace
            }],
            role_ref=V1RoleRef(api_group="rbac.authorization.k8s.io", kind="Role", name=role_name)
        )
        rbac_api_instance.create_namespaced_role_binding(namespace=target_namespace, body=rb_body)


     # Create ConfigMap with ServiceAccount and Secret information
    configmap_name = "iamkube-configmap"
    iamkube_namespace = "iamkube"  # namespace for ConfigMap
    new_data_value = f"SecretName: {secret_name}\nServiceAccountName: {sa_name}\nRoleBindingName: {rb_name if role_name else 'N/A'}"

    try:
        existing_cm = api_instance.read_namespaced_config_map(name=configmap_name, namespace=iamkube_namespace)
        if existing_cm.data is None:
            existing_cm.data = {}
        existing_cm.data[name] = new_data_value
        api_instance.patch_namespaced_config_map(name=configmap_name, namespace=iamkube_namespace, body=existing_cm)
    except kubernetes.client.rest.ApiException as e:
        if e.status == 404:
            configmap_body = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(name=configmap_name, namespace=iamkube_namespace),
                data={name: new_data_value}
            )
            api_instance.create_namespaced_config_map(namespace=iamkube_namespace, body=configmap_body)
        else:
            logging.error(f"An error occurred while creating ConfigMap: {e}")


    # Ensure the finalizer is not already added
    finalizers = meta.get('finalizers', [])
    if FINALIZER_NAME not in finalizers:
        finalizers.append(FINALIZER_NAME)


    # Logging setup completion
    logging.info(f"IamKube {name} setup completed. ServiceAccount {sa_name}, Secret, RoleBinding have been configured.")

    # Update the status to reflect successful creation
    now = datetime.now(timezone.utc).isoformat()
    status_update = {
        "status": {
            "conditions": [{
                "lastTransitionTime": now,
                "message": "Service account, secret, and role binding created successfully.",
                "reason": "CreationSucceeded",
                "status": "True",
                "type": "Ready",
            }],
            "serviceAccountCreated": True,
            "secretCreated": True,
            "roleBindingCreated": True
        }
    }
    
    # Patch the status of the custom resource
    custom_api.patch_namespaced_custom_object_status(
        group="iamkube.local",
        version="v1",
        namespace=meta['namespace'],
        plural="iamkubes",
        name=meta['name'],
        body=status_update,
    )
    # Update the status of the custom resource
    #kopf.adopt(spec)
    #kopf.append_status(spec, {"resources_created": ["ServiceAccount", "Secret", "RoleBinding"]})


@kopf.on.update('iamkube.local', 'v1', 'iamkubes')
def update_iamkube(spec, name, meta, **kwargs):
    username = spec.get('username')
    target_namespace = spec.get('namespace', 'default')
    expiry_days = int(spec.get('expiry', 0))
    role_name = spec.get('role', None)
    sa_name = serviceaccount_name(username, name)

    # Check if the ServiceAccount exists, if not, create it
    try:
        api_instance.read_namespaced_service_account(sa_name, namespace=target_namespace)
    except kubernetes.client.rest.ApiException as e:
        if e.status == 404:
            sa_body = client.V1ServiceAccount(metadata=client.V1ObjectMeta(name=sa_name))
            api_instance.create_namespaced_service_account(namespace=target_namespace, body=sa_body)
        else:
            logging.error(f"Error occurred while checking ServiceAccount existence: {e}")
            return

    # Update Secret expiry if it exists
    secret_name = f"{sa_name}-secret"
    try:
        secret = api_instance.read_namespaced_secret(secret_name, namespace=target_namespace)
        expiry_time = datetime.utcnow() + timedelta(days=expiry_days)
        secret.metadata.annotations["expiration"] = expiry_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        api_instance.replace_namespaced_secret(name=secret_name, namespace=target_namespace, body=secret)
    except kubernetes.client.rest.ApiException as e:
        if e.status == 404:
            logging.error(f"Secret {secret_name} not found. Cannot update expiry.")
        else:
            logging.error(f"Error occurred while updating Secret expiry: {e}")

    # Check if the RoleBinding exists,
    # If role_name is not None, delete the existing RoleBinding and create a new one
    if role_name:
        rb_name = f"{sa_name}-rb"
        try:
            existing_rb = rbac_api_instance.read_namespaced_role_binding(rb_name, namespace=target_namespace)
            if existing_rb:
                rbac_api_instance.delete_namespaced_role_binding(rb_name, namespace=target_namespace)
        except kubernetes.client.rest.ApiException as e:
            if e.status != 404:
                logging.error(f"Error occurred while checking RoleBinding existence: {e}")
                return

        rb_body = V1RoleBinding(
            metadata=V1ObjectMeta(name=rb_name, namespace=target_namespace),
            subjects=[{
                "kind": "ServiceAccount",
                "name": sa_name,
                "namespace": target_namespace
            }],
            role_ref=V1RoleRef(api_group="rbac.authorization.k8s.io", kind="Role", name=role_name)
        )
        rbac_api_instance.create_namespaced_role_binding(namespace=target_namespace, body=rb_body)

    logging.info(f"IamKube {name} update completed. ServiceAccount {sa_name}, Secret expiry, RoleBinding have been updated.")

    # Update the status to reflect successful update
    now = datetime.now(timezone.utc).isoformat()
    status_update = {
        "status": {
            "conditions": [{
                "lastTransitionTime": now,
                "message": "Service account, secret, and role binding updated successfully.",
                "reason": "UpdateSucceeded",
                "status": "True",
                "type": "Ready",
            }],
            "serviceAccountUpdated": True,
            "secretUpdated": True,
            "roleBindingUpdated": True
        }
    }

    # Patch the status of the custom resource
    custom_api.patch_namespaced_custom_object_status(
        group="iamkube.local",
        version="v1",
        namespace=meta['namespace'],
        plural="iamkubes",
        name=meta['name'],
        body=status_update,
    )

@kopf.on.delete('iamkube.local', 'v1', 'iamkubes')
def delete_iamkube(spec, name, meta, **kwargs):
    username = spec.get('username')
    target_namespace = spec.get('namespace', 'default')
    sa_name = serviceaccount_name(username, name)
    secret_name = f"{sa_name}-secret"
    rb_name = f"{sa_name}-rb"
    configmap_name = "iamkube-configmap"  # Assuming this is the name of your ConfigMap
    iamkube_namespace = "iamkube"  # Assuming this is the namespace of your ConfigMap


    # Delete the Secret
    try:
        api_instance.delete_namespaced_secret(name=secret_name, namespace=target_namespace)
        logging.info(f"Deleted Secret: {secret_name}")
    except kubernetes.client.rest.ApiException as e:
        if e.status != 404:  # Not Found
            logging.error(f"Error occurred while deleting Secret: {e}")

    # Delete the RoleBinding
    try:
        rbac_api_instance.delete_namespaced_role_binding(name=rb_name, namespace=target_namespace)
        logging.info(f"Deleted RoleBinding: {rb_name}")
    except kubernetes.client.rest.ApiException as e:
        if e.status != 404:  # Not Found
            logging.error(f"Error occurred while deleting RoleBinding: {e}")

    # Delete the ServiceAccount
    try:
        api_instance.delete_namespaced_service_account(name=sa_name, namespace=target_namespace)
        logging.info(f"Deleted ServiceAccount: {sa_name}")
    except kubernetes.client.rest.ApiException as e:
        if e.status != 404:  # Not Found
            logging.error(f"Error occurred while deleting ServiceAccount: {e}")
    
    # Attempt to read the ConfigMap
    try:
        config_map = api_instance.read_namespaced_config_map(name=configmap_name, namespace=iamkube_namespace)
        
        # Check if the entry exists and remove it
        if name in config_map.data:
            del config_map.data[name]

            # Replace the entire ConfigMap with the updated data
            api_instance.replace_namespaced_config_map(
                name=configmap_name, 
                namespace=iamkube_namespace, 
                body=config_map
            )
            logging.info(f"Entry for {name} removed from ConfigMap {configmap_name}.")
        else:
            logging.info(f"No entry for {name} found in ConfigMap {configmap_name}.")

    except kubernetes.client.rest.ApiException as e:
        logging.error(f"Failed to update ConfigMap {configmap_name}: {e}")


    logging.info(f"Successfully deleted resources for IamKube: {name}")


    # Remove finalizer
    if FINALIZER_NAME in meta.get('finalizers', []):
        kopf.remove_finalizer(meta, FINALIZER_NAME)
        logging.info(f"Finalizer removed for {name}, resource can now be deleted")
