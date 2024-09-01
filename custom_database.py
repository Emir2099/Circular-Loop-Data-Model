import bson
import os

class CustomDatabase:
    def __init__(self, db_file_path):
        self.db_file_path = db_file_path
        self.data = self.load()

    def load(self):
        if os.path.exists(self.db_file_path):
            try:
                with open(self.db_file_path, 'rb') as file:
                    data = file.read()
                    if data:  # Ensure there's data to decode
                        return bson.loads(data)
                    else:
                        print("Warning: BSON file is empty.")
                        return {}
            except Exception as e:
                print(f"Error loading BSON data: {e}")
                return {}
        else:
            print(f"Error: File {self.db_file_path} does not exist.")
            return {}

    def save(self):
        try:
            with open(self.db_file_path, 'wb') as file:
                file.write(bson.dumps(self.data))
        except Exception as e:
            print(f"Error saving BSON data: {e}")

    def clear(self):
        self.data = {}
        self.save()
