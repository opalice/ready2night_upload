# ready2night_upload

Upload API service for Ready2Night album/media ingestion and processing.

## Endpoints
- `GET /v1/health`
- `POST /v1/admin/rooms/:slug/albums/upload?album=...`
- `GET /v1/admin/upload/queues`
- `GET /v1/admin/upload/jobs/:id`
- `GET /v1/admin/upload/jobs/:id/events`
- `POST /v1/admin/upload/jobs/:id/retry`
- `POST /v1/admin/upload/jobs/:id/cancel`

## Notes
- Uses SQLite queue + background worker.
- Converts images to WEBP + AVIF.
- Converts videos to MP4 + WEBP poster.
