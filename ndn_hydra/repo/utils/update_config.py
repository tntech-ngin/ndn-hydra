import yaml

def update_config_file():
  with open("config.yaml","r") as yamlfile:
    default_config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    
    data[0]['repo_prefix'] = args["repo_prefix"]
    data[0]['node_name'] = args["node_name"]
    data[0]['data_storage_path'] = args["data_storage_path"]
    data[0]['global_view_path'] = args["global_view_path"]
    data[0]['rtt'] = random.randint(1, 100)
    data[0]['num_users'] = random.randint(1, 10)
    data[0]['bandwidth'] = random.randint(1, 500)
    data[0]['network_cost'] = random.randint(1, 100)
    data[0]['storage_cost'] = random.randint(1, 100)
    data[0]['remaining_storage'] = random.randint(1, 1000)
  
  yamlfile.close()