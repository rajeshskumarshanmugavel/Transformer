
  

# migrateGenie Transformer

  

> Web application that processes a request from the **Controller** which sends a migration message from the queue, transforms and processes them

  

  

### To run the application

  

- Add the .env file

  

  

```

  

EXEC_ENVIRONMENT='development'

  

AZURE_STORAGE_CONTAINER_NAME='xxx'

  

AZURE_CONNECTION_STRING='xxx'

  

```

  

- Install all the requirements

  

```

  

pip install -r requirements.txt

  

```

  

- Run the server

  

```

  

./scripts/run.sh

  

```

### To test it locally

- Set `EXEC_ENVIRONMENT` as `local`

- Set `TRANSFORMATION_JSON_FILE_PATH` to a valid file inside `test/data`
