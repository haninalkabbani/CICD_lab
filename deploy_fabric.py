import os
from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace, publish_all_items

tenant_id = os.environ["TENANT_ID"]
client_id = os.environ["CLIENT_ID"]
client_secret = os.environ["CLIENT_SECRET"]

credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret,
)

target_workspace = FabricWorkspace(
    workspace_name="Fabric_lab_1_Prd",
    repository_directory=".",
    environment="PROD",
    token_credential=credential,
)

publish_all_items(target_workspace)
