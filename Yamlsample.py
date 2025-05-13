import yaml

def read_and_print_yaml(file_path):
    """Reads a YAML file, iterates through the 'items' list, and prints each element."""
    try:
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            if 'items' in data and isinstance(data['items'], list):
                for item in data['items']:
                    print(item)
            else:
                print("The YAML file does not contain a list named 'items', or 'items' is not a list.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}")

if __name__ == "__main__":
    yaml_file_path = 'data.yaml'  # Specify the path to your YAML file
    read_and_print_yaml(yaml_file_path)
