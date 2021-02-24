from productions import ProductionStore

def test_productions(name=None):
    from random import choice
    from string import ascii_lowercase

    production_store = ProductionStore(production_store_id="91566e5d-9644-48b4-b664-1b3c6f744af7")

    if name:
        production_name = name
    else:
        production_name = ''.join(choice(ascii_lowercase) for i in range(12))

        print(f"TEST: create production: {production_name}")
        production_store.create_production(production_name)

    print("TEST: list productions")

    production_store.create_production(name)
    print(production_store.list_productions())

    print("TEST: get production metadata")
    print(production_store.get_production_metadata(production_name))

    print("TEST: reset upload PIN")
    production_pin = production_store.set_production_upload_pin(production_name)

    print("TEST: get ingest url")
    print(production_store.get_ingest_url(production_name=production_name, pin=production_pin))

    print("TEST: get production tree")
    print(production_store.get_production_tree(production_name=production_name))

    print("TEST: update wip production tree")
    production_store.update_wip_production_tree(production_name)

    print("TEST: get wip production tree")
    wip_production_tree = production_store.get_wip_production_tree(production_name=production_name)
    print(wip_production_tree)

    print("TEST: update production store metadata blob with wip production tree")
    print(production_store.set_production_tree(production_name=production_name, production_tree=wip_production_tree))

    print("TEST: get SAS token from files")
    print(production_store.get_files_sas_url(production_name=production_name))


def test_copy_functions(name = None):
    production_store = ProductionStore(production_store_id="91566e5d-9644-48b4-b664-1b3c6f744af7")
    if name:
        production_name = name
    else:
        production_name = ''.join(choice(ascii_lowercase) for i in range(12))

        print(f"TEST: create production: {production_name}")
        production_store.create_production(production_name)

    print("TEST: get ingest SAS url")
    ingest_url = production_store.get_blob_sas_url(production_name=production_name)
    print (f"ingest_url = '{ingest_url}'")

    print("TEST: get files SAS url")
    files_url = production_store.get_files_sas_url(production_name=production_name)
    print (f"files_url = '{files_url}'")

    print("TEST: copy from blob to files")
    production_store.copy_production_to_files(production_name=name)

    print("TEST: copy from blob to files")
    production_store.copy_production_to_blob(production_name=name)



test_productions(name="qanbdvtzvxmp")
test_copy_functions(name = "qanbdvtzvxmp")