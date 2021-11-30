 gcloud functions deploy cf_callbacks  \
  --region europe-west1 \
  --entry-point entry_point \
  --runtime python37 \
  --memory 128MB \
  --trigger-event "providers/cloud.firestore/eventTypes/document.create" \
  --trigger-resource "projects/kredoh/databases/(default)/documents/callbacks_test/{id}"