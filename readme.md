# Building and Running the Docker Container

## Build the Docker Image

To build the Docker image, navigate to the directory containing the `Dockerfile` and run the following command:

```sh
docker build -t fire_incident_pipeline .
```

## Run the Docker Container

Once the image is built, you can run the container using the following command:

```sh
docker run --rm -v "$(pwd):/app" fire_incident_pipeline
```

This will start the container in detached mode and map port 8080 of the container to port 8080 on your host machine.

