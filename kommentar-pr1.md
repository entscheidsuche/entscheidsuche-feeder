Hi @clementsicard,

Thank you very much for the thorough analysis and the PR. I'm sorry it took a while.

I've taken over the `HTTPSFileLoader` part in a separate commit (with you as co-author). Your observation there is more important than the PR description suggests: without an `error` listener on the `ClientRequest`, a network-level failure (connection reset, refused connection, DNS error) does not just leave a stream hanging. It raises an unhandled `error` event and crashes the whole feeder process. With the fix, such failures surface as a regular per-document error that includes the file name.

I'm closing the rest of the PR without merging, for the following reasons:

1. **Failed documents are already handled one level up.** The consolidator (Konsolidierer) evaluates the error returned by the feeder, identifies the failing document, and restarts the job without it. So an error in one document does not leave the rest of the job unindexed permanently. The 12 BGer decisions from job 2815 are all in the index now (e.g. 7B_68/2026 and 9C_643/2025).
2. **The changed error format would break that mechanism.** The PR replaces the per-document error (`{document, response}` or `error building file …`) with an aggregated object `{message, errors: [...]}`. The consolidator relies on the current format to identify the document to skip.
3. **The retries also cover deterministic errors** (404, invalid JSON, Elasticsearch mapping errors), where a retry cannot help, and they make an Elasticsearch outage much slower to detect, because every document is attempted three times instead of failing fast.
4. **The aggregated error list is sent unbounded as a GET query parameter** to the status endpoint, which can exceed URL length limits on larger failures.

Apart from that, the feeder will be restructured anyway in the course of the new AI search, which is going live soon. Larger changes to the current feeder are therefore not worth the effort at this point; the error handling will be reconsidered as part of that rework.

Until then, small, self-contained fixes are still welcome, e.g. a timeout on the HTTPS request (`request.setTimeout` plus `destroy`) and forwarding errors that occur on the response stream after the transfer has started.

Thanks again for digging into this!
