import logging
import azure.functions as func
import uuid
import configparser

class ProductionStore():
    def __init__(self, blob_connection_string=None, file_connection_string=None, production_store_id=None, production_store_name=None):
        import os
        import json

        config = configparser.RawConfigParser()   
        config.read(os.path.join(os.path.dirname(__file__), 'productionstore.cfg'))

        from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
        from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
        from azure.storage.fileshare import ShareClient, ResourceTypes, AccountSasPermissions

        if blob_connection_string:
            self.blob_connection_string = blob_connection_string
        else:
            self.blob_connection_string = str(config.get('storage account credentials', 'blob_storage_connection_string'))
        
        if file_connection_string:
            self.file_connection_string = file_connection_string
        else:
            self.file_connection_string = config.get('storage account credentials', 'file_storage_connection_string')
        
        self.production_store_id = production_store_id
        self.production_store_name = production_store_name

        # global, name for all metadata files at the root of each production prefix
        self.metadata_file_name = config.get('global settings', 'metadata_filename')
        
        # global, prefix for ingest location in each production
        self.ingest_prefix = config.get('global settings', 'ingest_path')

        # global, default for directory structures
        self.default_production_tree = json.loads(config.get('production defaults', 'default_tree'))

        # blob storage connection
        logging.info("init connection to blob storage")
        self.blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)

        if production_store_id:
            self.get_production_store()
        else:
            self.create_production_store()

        # file service connetion
        logging.info("init connection to file service")
        self.share_client = ShareClient.from_connection_string(self.file_connection_string, share_name=self.production_store_name)

            
    def metadata(self):
        # return a dict of strings of metadata stored in the object, to be stored as container metadata
        return dict({
                'production_store_name': str(self.production_store_name),
                'production_store_id' : str(self.production_store_id)
                })

    def get_production_store(self):
        # Read a 'Production Store' container and it's metadata
        container_client = self.blob_service_client.get_container_client(self.production_store_id)
        container_properties = container_client.get_container_properties()
        try:
            self.production_store_name = container_properties.metadata['production_store_name']
        except:
            logging.error("Error")

    def create_production_store(self):
        from azure.core.exceptions import ResourceExistsError
        
        if self.production_store_name:
            # Create a unique name for the container
            self.production_store_id = str(uuid.uuid4())

            # Create the container
            logging.info(f"Creating Blob Container for production store name '{self.production_store_name}' named '{self.production_store_id}'")
            container_client = self.blob_service_client.get_container_client(str(self.production_store_id))

            try:
                container_client.create_container()
            except ResourceExistsError :
                logging.error(f"Container '{self.production_store_id}' named already exists")
    
            container_client.set_container_metadata(metadata=self.metadata())
        else:
            logging.error("Missing parameter: production_store_name")
    
    def list_productions(self):
        container_client = self.blob_service_client.get_container_client(str(self.production_store_id))
        blob_list = container_client.walk_blobs()
        productions = []
        for blob in blob_list:
            productions.append(blob.name[:-1])
        return productions

    def create_production(self, production_name, production_tree=None):
        import json
        # create a Production by creating an prefix in the container, containging one file named '.production' that file will contain metadata of the Production as Blob metadata, and contain a complete listing of the directory it is sycning from Files. 
        production_id = str(uuid.uuid4())
        container_client = self.blob_service_client.get_container_client(str(self.production_store_id))
        logging.info("creating projection named '{production_name}'")
        try:
            if production_tree:
                blob_data = json.dump(production_tree)
            else:
                # use the class default production
                blob_data = json.dumps(self.default_production_tree)

            production_metadata = {"production_id":production_id, "online":"False"}
            blob_name = f"{production_name}/{self.metadata_file_name}"
            metadata_blob = container_client.upload_blob(data=blob_data, name=blob_name, metadata=production_metadata)
        except azure.core.exceptions.ResourceExistsError:
            logging.error(f"The production named '{production_name}' already exists.")

    def get_production_metadata(self, production_name):
        # return metadata about a production
        from azure.core.exceptions import ResourceNotFoundError
        try:
            container_client = self.blob_service_client.get_container_client(str(self.production_store_id))
            metadata_blob_name = f"{production_name}/{self.metadata_file_name}"
            metadata_blob_client = container_client.get_blob_client(metadata_blob_name)
            return metadata_blob_client.get_blob_properties().metadata
        except ResourceNotFoundError as e:
            logging.error(f"metadata blob: '{metadata_blob_name}' for production '{production_name}' is missing")
            raise e

    def get_production_tree(self, production_name):
        import json
        # return the tree from the production medata blob about a production
        from azure.core.exceptions import ResourceNotFoundError
        try:
            container_client = self.blob_service_client.get_container_client(str(self.production_store_id))
            metadata_blob_name = f"{production_name}/{self.metadata_file_name}"
            metadata_blob_client = container_client.get_blob_client(metadata_blob_name)
            production_tree_json = metadata_blob_client.download_blob().readall()
            production_tree = json.loads(production_tree_json.decode("utf-8"))
            return production_tree
        except ResourceNotFoundError as e:
            logging.error(f"metadata blob: '{metadata_blob_name}' for production '{production_name}' is missing")
            raise e

    def set_production_tree(self, production_name, production_tree):
        # TODO write the tree back to a metadata blob
        import json
        container_client = self.blob_service_client.get_container_client(str(self.production_store_id))
        production_metadata = self.get_production_metadata(production_name)
        blob_data = json.dumps(production_tree)
        blob_name = f"{production_name}/{self.metadata_file_name}"
        metadata_blob = container_client.upload_blob(data=blob_data, name=blob_name, metadata=production_metadata, overwrite=True)

    def set_production_upload_pin(self, production_name):
        from random import choice
        from string import digits
        from azure.core.exceptions import ResourceExistsError
        container_client = self.blob_service_client.get_container_client(str(self.production_store_id))
        # return a PIN to obtain a upload link, this is stored in metadata on the .metadata blon and verified with verify_production_upload_pin which returns the URL to the Ingest prefix on the blob
        upload_pin = ''.join(choice(digits) for i in range(6))
        try:
            metadata_blob_client = container_client.get_blob_client(f"{production_name}/{self.metadata_file_name}")
            production_metadata = metadata_blob_client.get_blob_properties().metadata
            production_metadata['upload_pin'] = upload_pin
            metadata_blob_client.set_blob_metadata(metadata=production_metadata)
            return upload_pin
        except ResourceExistsError as e:
            logging.error(f"The production named '{production_name}' already exists.")
            raise e

    def get_blob_sas_url(self, production_name=None, path=None):
        from datetime import datetime, timedelta
        from azure.storage.blob import generate_account_sas, generate_container_sas, ResourceTypes, AccountSasPermissions

        container_client = self.blob_service_client.get_container_client(self.production_store_id)

        sas_token = generate_account_sas(
            container_client.account_name,
            resource_types=ResourceTypes(service=True, object=True, container=True),
            permission=AccountSasPermissions(read=True, write=True, delete=True, list=True, add=True, create=True, update=True, process=True, delete_previous_version=False),
            expiry=datetime.utcnow() + timedelta(hours=24),
            account_key=self.blob_service_client.credential.account_key,
        )

        if path:
            return f"https://{container_client.account_name}.blob.core.windows.net/{self.production_store_id}/{production_name}/{path}?{sas_token}"
        else:
            return f"https://{container_client.account_name}.blob.core.windows.net/{self.production_store_id}/{production_name}?{sas_token}"


    def get_ingest_url(self, production_name, pin):
        # verity upload pin, return URL
        # try:
        logging.info(f"getting metadata for production '{production_name}'")
        production_metadata = self.get_production_metadata(production_name)
        if pin == production_metadata['upload_pin']:
            #pin matches, return URL
            return self.get_blob_sas_url(production_name=production_name, path=self.ingest_prefix)
        else:
            logging.error("Upload PIN missmatch")
        # except:
            # logging.error("No upload PIN has been set")

    ## Files operations

    def update_wip_production_tree(self, production_name):
        from azure.core.exceptions import ResourceExistsError
        # create any directories in the WIP directory from the tree stored as metadata on the production
        
        # get the tree from the production metadata blob
        production_tree = self.get_production_tree(production_name)

        #print(production_tree)

        # create folders in the files location
        logging.info(f"Creating production directory '{production_name}'")
        try:
            directory_client = self.share_client.create_directory(directory_name=production_name)
        except ResourceExistsError:
            print(f"Directory '{production_name}'exists, skipping")
            directory_client = self.share_client.get_directory_client(directory_path=production_name)
        
        self.create_wip_directory(directory_client=directory_client, sub_tree=production_tree[0]['contents']) # production tree will only contain one directory at the root
        

    def create_wip_directory(self, directory_client, sub_tree):
        print(">>>> CREATING SUBTREE")
        print(directory_client.directory_path)
        print(sub_tree)
        from azure.core.exceptions import ResourceExistsError 
        # iterable function to reate subdirectories on the file shares
        for file_or_dir in sub_tree:
            if file_or_dir['type'] == 'directory':
                logging.info(f"Creating sub directory '{file_or_dir['name']}'")
                try:
                    directory_name = file_or_dir['name']
                    new_directory_client = directory_client.create_subdirectory(directory_name=directory_name)
                except ResourceExistsError:
                    logging.info(f"Directory '{directory_name}' exists, skipping")
                    new_directory_client = directory_client.get_subdirectory_client(directory_name=directory_name)

                # if the directory is not empty, then recurse into it
                if 'contents' in file_or_dir:
                    self.create_wip_directory(directory_client=new_directory_client, sub_tree=file_or_dir['contents'])
    

    def get_wip_production_tree(self, production_name):
        # return a tree of the production wip directory 
        return [({'name':production_name, 'type':'directory', 'contents':self.get_wip_directory(production_name)})]

    def get_wip_directory(self, path=""):
        from azure.core.exceptions import ResourceNotFoundError 
        try:
            file_dir_list = list(self.share_client.list_directories_and_files(directory_name=path))
        except azure.core.exceptions.ResourceNotFoundError as e:
            logging.error(f"Directory does not exist '{self.share_client.share_name}/{directory_name}', error: '{e}'")
        name = path.split('/')[-1]

        tree = []

        for file_or_dir in file_dir_list:
            name = file_or_dir['name']
            if not file_or_dir['is_directory']:
                tree.append({'name':name, 'type':'file'})
            else:
                if path != "":
                    dir_path = f"{path}/{name}"
                else:
                    dir_path = name
                tree.append({'name':name, 'type':'directory', 'contents':self.get_wip_directory(dir_path)})
        return tree

    def get_files_sas_url(self, production_name=None, path=None):
        from datetime import datetime, timedelta
        from azure.storage.blob import generate_container_sas
        from azure.storage.fileshare import ShareServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions

        sas_token = generate_account_sas(
            account_name=self.share_client.account_name,
            account_key=self.share_client.credential.account_key,
            resource_types=ResourceTypes(service=True, container=True, object=True),
            protocol='https',
            permission=AccountSasPermissions(read=True, write=True, delete=True, list=True, add=True, create=True, update=True, process=True, delete_previous_version=False),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
        if production_name and path:
            return f"https://{self.share_client.account_name}.file.core.windows.net/{self.production_store_name}/{production_name}/{path}?{sas_token}"
        elif production_name and not path:
            return f"https://{self.share_client.account_name}.file.core.windows.net/{self.production_store_name}/{production_name}?{sas_token}"
        elif path and not production_name:
            logging.error("must pass 'production name' parameter wwith 'path' parameter")
        else:
            return f"https://{self.share_client.account_name}.file.core.windows.net/{self.production_store_name}?{sas_token}"


        ## sync operations

    def azcopy_copy(self, source, dest):
        import subprocess
        command_exec = "/usr/local/bin/azcopy"
        command_params = [command_exec, "copy", source, dest, "--recursive", "--s2s-detect-source-changed"]
        print(f"Tyring: {' '.join(command_params)}")
        process_return = subprocess.run(command_params)
        print(process_return)

    def azcopy_sync(self, source, dest):
        # don't use yet, sync files <=> blob not supported in azcopy v10
        import subprocess
        command_exec = "/usr/local/bin/azcopy"
        command_params = [command_exec, "sync", source, dest, "--recursive"]
        print(f"Tyring: {' '.join(command_params)}")
        process_return = subprocess.run(command_params)
        print(process_return)


        # sync new ingests to files

        # sync files to blob
    
    def copy_production_to_blob(self, production_name=None):
        # copy produciton storage on files to blob

        # copy the files:
        # get blob SAS url
        blob_url = self.get_blob_sas_url(production_name=production_name)

        # get files SAS url
        files_url = self.get_files_sas_url(production_name=production_name, path="*")

        logging.info(self.azcopy_copy(files_url, blob_url))

        # get the current WIP tree on the files volume 
        wip_production_tree = self.get_wip_production_tree(production_name=production_name)
        print(wip_production_tree)

        # update production store metadata blob with wip production tree
        print(self.set_production_tree(production_name=production_name, production_tree=wip_production_tree))

    def copy_production_to_files(self, production_name=None):
        # restores blob storage back to production storage

        # recreate the folder structure on the files volume from what was stored in the blob storage
        self.update_wip_production_tree(production_name=production_name)

        # copy the files:
        # get blob SAS url
        blob_url = self.get_blob_sas_url(production_name=production_name)

        # get files SAS url
        files_url = self.get_files_sas_url()

        # start the copy process
        logging.info(self.azcopy_copy(files_url, blob_url))

        # get the current WIP tree on the files volume
        wip_production_tree = self.get_wip_production_tree(production_name=production_name)

        # update production store metadata blob with wip production tree - there shouldn't be any changes
        self.set_production_tree(production_name=production_name, production_tree=wip_production_tree)


        # TODO delete blobs that were deleted from files
        # TODO detect folder movements and update paths only, not copy
        # TODO sync blob to files

        
# create a new production store
# production_store = ProductionStore(blob_connection_string=azstorage_connection_str, production_store_name="New Store"
