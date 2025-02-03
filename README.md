# Cage Algorithm Template

This repository is a template from which one can start to develop an algorithm to be run in a cage in the Datavillage DCP.
To implement your own algorithm, clone this repo and edit `process.py`. To test, run `python process.py` from the root folder with the correct environment variables set (cfr `.env.example`).

## Content

Here is a list of the files in this repo and what their use is

| File name                           | Usage                                                                                                                                                           |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| requirements.txt                    | The python dependencies needed to run the code.                                                                                                                 |
| process.py                          | The implementation of the algorithm. It contains a method `event_processor(evt: dict)` where processing begins.                                                 |
| index.py                            | Entry point of the code when it's ran in a cage. It starts an event listener that registers the incoming events and passes them to the `event_processor` method |
| Dockerfile                          | Bundles the project in a self contained Docker image                                                                                                            |
| .github/workflows/release-image.yml | Builds and pushes the image using github actions                                                                                                                |

## Events

Following events are supported

```json
{
  "type": "EX_READ_SPACE"
}
```

Reads the details of the space the cage it is deployed for, and prints a summary.

```json
{
  "type": "EX_READ_BUCKET",
  "gcs": {
    "location": "gs://cage_example/netflix_titles.csv", // optional
    "keyId": "GCS_KEY_ID",
    "secret": "GCS_SECRET"
  }
}
```

Reads a file from GCS bucket. If `location` is not specified, it will read the default specified in `config.GCS_DEFAULT_READ`.

```json
{
  "type": "EX_WRITE_BUCKET",
  "gcs": {
    "location": "gs://cage_example", // optional
    "keyId": "GCS_KEY_ID",
    "secret": "GCS_SECRET"
  },
  "data": {
    "hello": "world",
    "from": "cage"
  }
}
```

Writes the `data` dict as a json file to GCS bucket. If `location` is not specified, it will write by default to `config.GCS_DEFAULT_WRITE`.

```json
{
  "type": "EX_WRITE_BUCKET_SIGNED",
  "download": true, // optional. Default `false`
  "gcs": {
    "location": "gs://cage_example", // optional
    "keyId": "GCS_KEY_ID",
    "secret": "GCS_SECRET"
  },
  "data": {
    "hello": "world",
    "signed": "data"
  }
}
```

Creates a json from the dict under `data` and signs it at the secret manager. The result is written to a GCS bucket.
If `location` is not specified, it will write by default to `config.GCS_DEFAULT_WRITE`.
If `download` is true, it will download the signed file to `config.DATA_FOLDER`.

```json
{
  "type": "EX_DECRYPT_FILE",
  "gcs": {
    "location": "gs://cage_example", // optional
    "keyId": "GCS_KEY_ID",
    "secret": "GCS_SECRET"
  },
  "message": {
    "passphrase": "PASSPHRASE",
    "content": "CONTENT"
  }
}
```

Decrypts the encrypted content under `message` at the secret manager and writes the result to a GCS bucket.
If `location` is not specified, it will write by default to `config.GCS_DEFAULT_WRITE`.

## Run in cage

When the algorithm is deployed, the listener created in `index.py` will start and listen to the Redis queue for events.
When an event is received, it is first registered in the logging module. This makes sure the event is printed with every log statement from the execution of that event.
After that, the event is passed to the `event_processor` method that is passed in the constructor.

## Local development

To test the algorithm locally, use the `dispatch_event_local(evt: dict)` method. This will make sure logging is properly configured as it would be in the cage. In the cage, this functionality is implemented by the listener.
To have the correct `app_id` set in the logs, run the code in an environment with the variable `DV_CAGE_ID` set.
To run the code, simply run `python process.py`.
