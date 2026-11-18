# Adding a Provider

Add a built-in channel under `provider/<name>`. Depend on the shared contracts,
not another provider or a concrete MA Adapter.

External custom channels implement `channel.Provider` and wrap it with
`channel.NewProvider`. Start forwards inbound facts to accept; Deliver sends the
result using the stable UUID and reply target. Both methods must honor ctx. Keep
leases, persistence cursors, and credentials out of message fields.

Optional capabilities use separate interfaces: AttachmentFetcher, HistoryFetcher,
ReferenceFetcher, ProgressReporter, DurableProgressReporter, ApprovalPrompter,
ControlBinder, ArtifactDeliverer, InlineImageUploader, InlineMediaUploader,
RichTextDeliverer, and BotTokenSource. Implement only the capabilities you support.
The public delivery view contains text, file, and rich-text references without
exposing persisted Delivery records. InlineMediaUploader requires
RichTextDeliverer for the resulting inline media to be delivered.

Verify the callback source, application, conversation, and operator before
submitting an approval through Controller. Do not trust identity claims in a card
payload. Use DeliveryFailure to distinguish a definite retryable rejection from
an unknown send outcome, preventing duplicate sends after network failures.

The built-in Feishu provider implements specialized native final-answer streaming
and progress protocols. The initial public extension API does not expose every
internal protocol: NewRuntime rejects StreamFinalAnswer for a custom provider
without the required support. A future built-in DingTalk provider can compose
internal capabilities within the module and should have reproducible contract and
integration coverage for each supported capability.
