# Examples

Runnable examples for the `arkruntime` Go SDK. Each expects `ARK_API_KEY` in the env:

```bash
export ARK_API_KEY=...
go run examples/responses/basic/main.go
```

| Dir | What it shows |
|---|---|
| responses/basic/ | basic streaming responses with two-round conversation on POST /v1/responses |
| listinputitems/ | GET /v1/responses/{id}/input_items |
| multimodalembeddings/ | POST /embeddings/multimodal with an image input |
| sparseembeddings/ | POST /embeddings/multimodal with sparse embedding output enabled |
| images/ | POST /images/generations — Seedream T2I, Seededit edit-from-image, sequential image generation |
| agents/ | Managed-Agents: Agent lifecycle — Create/Get/List/Update/ListVersions/Delete |
| environments/ | Managed-Agents: Environment lifecycle — Create/Get/List/Update/Delete (cloud + unrestricted networking) |
| sessions_loop/ | Managed-Agents: end-to-end agent loop — Agent + Env + Session, send user.message, stream events until idle |
| memory_stores/ | Managed-Agents: MemoryStore + nested Memory CRUD |

The Managed-Agents examples additionally accept `ARK_MODEL_ID` for the model id (falls back to a `${YOUR_MODEL_ID}` placeholder that will 400 at runtime).

Only currently-implemented APIs have runnable examples. See the `API Coverage` table in the top-level README for the roadmap.
