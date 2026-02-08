import { ImapFlow } from "imapflow";
import { storage } from "./storage";
import * as crypto from "crypto";
import {
  processEmailAttachment,
  combineAndImportStagedFiles,
  logSystemError,
} from "./importUtils";
import { triggerAutoConsolidationAfterImport } from "./routes";
import { triggerShopifySyncAfterImport } from "./scheduler";
import {
  sendEmailFetcherAlert,
  sendImportSuccessNotification,
} from "./errorReporter";

export interface EmailSettings {
  host: string;
  port: number;
  secure: boolean;
  username: string;
  password: string;
  folder: string;
  senderWhitelist: string[];
  subjectFilter: string;
  markAsRead: boolean;
  deleteAfterDownload?: boolean;
  extractLinksFromBody?: boolean;
  multiFileMode?: boolean;
  expectedFiles?: number;
}

export interface FetchResult {
  success: boolean;
  filesProcessed: number;
  errors: string[];
  logs: Array<{
    emailFrom: string;
    emailSubject: string;
    fileName: string;
    status: string;
    error?: string;
  }>;
}

function hashBuffer(buffer: Buffer): string {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

function isExcelFile(filename: string): boolean {
  const ext = filename.toLowerCase();
  return ext.endsWith(".xlsx") || ext.endsWith(".xls") || ext.endsWith(".csv");
}

export async function fetchEmailAttachments(
  dataSourceId: string,
  settings: EmailSettings,
): Promise<FetchResult> {
  const result: FetchResult = {
    success: true,
    filesProcessed: 0,
    errors: [],
    logs: [],
  };

  if (!settings.host || !settings.username || !settings.password) {
    result.success = false;
    result.errors.push(
      "Missing email configuration (host, username, or password)",
    );
    return result;
  }

  const client = new ImapFlow({
    host: settings.host,
    port: settings.port || 993,
    secure: settings.secure !== false,
    auth: {
      user: settings.username,
      pass: settings.password,
    },
    logger: false,
    socketTimeout: 30000,
  });

  client.on("error", (err: Error) => {
    console.error(
      `[Email Fetcher] IMAP client error for ${settings.host}:`,
      err.message,
    );
  });

  try {
    await client.connect();

    const folder = settings.folder || "INBOX";
    const lock = await client.getMailboxLock(folder);

    try {
      // First, try to search for unread emails
      let searchCriteria: any = { seen: false };

      // Note: For multiple senders, we fetch all unseen emails and filter client-side
      // This is because IMAP OR queries have varying support across servers
      if (settings.senderWhitelist && settings.senderWhitelist.length === 1) {
        const sender = settings.senderWhitelist[0].trim();
        if (sender) {
          searchCriteria.from = sender;
        }
      }
      // For multiple senders, whitelist filtering happens in the loop below

      console.log(
        `[Email Fetcher] Searching with criteria:`,
        JSON.stringify(searchCriteria),
      );
      let messagesResult = await client.search(searchCriteria, { uid: true });
      let messages = Array.isArray(messagesResult) ? messagesResult : [];
      console.log(
        `[Email Fetcher] Found ${messages.length} unread emails matching criteria`,
      );

      // If no unread emails found, also search recent emails (last 7 days) regardless of read status
      if (messages.length === 0) {
        const oneWeekAgo = new Date();
        oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

        const allSearchCriteria: any = { since: oneWeekAgo };
        if (settings.senderWhitelist && settings.senderWhitelist.length === 1) {
          const sender = settings.senderWhitelist[0].trim();
          if (sender) {
            allSearchCriteria.from = sender;
          }
        }

        console.log(
          `[Email Fetcher] No unread emails, searching all emails from last 7 days...`,
        );
        messagesResult = await client.search(allSearchCriteria, { uid: true });
        messages = Array.isArray(messagesResult) ? messagesResult : [];
        console.log(
          `[Email Fetcher] Found ${messages.length} total emails from last 7 days`,
        );
      }

      // Fetch all message envelopes to get dates and senders
      const messageInfos: Array<{
        uid: number;
        from: string;
        subject: string;
        date: Date | null;
      }> = [];

      for (const uid of messages) {
        try {
          const message = await client.fetchOne(
            uid,
            { envelope: true },
            { uid: true },
          );

          if (!message || !message.envelope) continue;

          const from = message.envelope.from?.[0]?.address || "unknown";
          const subject = message.envelope.subject || "";
          const emailDate = message.envelope.date || null;

          // Apply sender whitelist filter
          if (settings.senderWhitelist && settings.senderWhitelist.length > 0) {
            const isWhitelisted = settings.senderWhitelist.some((email) =>
              from.toLowerCase().includes(email.toLowerCase().trim()),
            );
            if (!isWhitelisted) continue;
          }

          // Apply subject filter
          if (
            settings.subjectFilter &&
            !subject
              .toLowerCase()
              .includes(settings.subjectFilter.toLowerCase())
          ) {
            continue;
          }

          messageInfos.push({ uid, from, subject, date: emailDate });
        } catch (err) {
          // Skip messages we can't fetch
          continue;
        }
      }

      // Sort by date (newest first)
      messageInfos.sort((a, b) => {
        if (!a.date && !b.date) return 0;
        if (!a.date) return 1; // No date goes to end
        if (!b.date) return -1;
        return b.date.getTime() - a.date.getTime(); // Newest first
      });

      // Process ALL emails from whitelisted senders (not just newest per sender)
      // This allows vendors to split files across multiple emails
      // SHA-256 hash deduplication below prevents reprocessing the same attachment twice
      const allWhitelistedMessages = messageInfos;

      console.log(
        `[Email Fetcher] Found ${allWhitelistedMessages.length} matching emails, processing all`,
      );

      const successfulUids = new Set<number>();
      let anyFileStagedGlobal = false;

      // Process all emails from whitelisted senders
      for (const msgInfo of allWhitelistedMessages) {
        const { uid, from, subject, date: emailDate } = msgInfo;

        try {
          console.log(
            `[Email Fetcher] Processing email from ${from} (${emailDate?.toISOString() || "no date"})`,
          );

          const fullMessage = await client.download(uid.toString(), undefined, {
            uid: true,
          });

          if (fullMessage && fullMessage.content) {
            const chunks: Buffer[] = [];
            for await (const chunk of fullMessage.content) {
              chunks.push(chunk);
            }
            const rawEmail = Buffer.concat(chunks);

            const attachments = await extractAttachments(client, uid);

            if (settings.extractLinksFromBody) {
              console.log(
                `[Email Fetcher] Extracting download links from email body...`,
              );
              const bodyFiles = await extractLinksFromEmailBody(client, uid);
              attachments.push(...bodyFiles);
              console.log(
                `[Email Fetcher] Total files after body extraction: ${attachments.length}`,
              );
            }

            // Get the last successful email date for this data source to enable date-aware duplicate detection
            // Use let so we can update after each successful import to prevent duplicate processing within same run
            let lastSuccessfulEmailDate =
              await storage.getLastSuccessfulEmailDate(dataSourceId);

            // Track if ANY file was successfully imported (for deferred sync)
            let anyFileImported = false;
            let anyFileStaged = false;

            // FIX: Detect multiple Excel files - force staging to prevent overwrites
            // When multiple files are downloaded from links in single-file mode,
            // each atomicReplace would delete the previous file's data.
            // Force staging so all files are combined at the end.
            const excelFileCount = attachments.filter((a) =>
              isExcelFile(a.filename),
            ).length;
            const forceStageMultiple = excelFileCount > 1;
            if (forceStageMultiple) {
              console.log(
                `[Email Fetcher] Detected ${excelFileCount} Excel files - forcing staging to combine all files`,
              );
            }

            for (const attachment of attachments) {
              if (!isExcelFile(attachment.filename)) continue;

              const fileHash = hashBuffer(attachment.content);

              const existingLogs = await storage.getEmailFetchLogsByHash(
                dataSourceId,
                fileHash,
              );
              // FIX: Only skip if a SUCCESSFUL import exists for this hash
              // Failed imports should be retryable
              const hasSuccessfulImport = existingLogs?.some(
                (log: any) => log.status === "success",
              );
              if (hasSuccessfulImport) {
                // Check if this email is NEWER than the last successful import
                // If so, allow reprocessing even with same hash (vendor may have sent same file in newer email)
                const isNewerEmail =
                  emailDate &&
                  lastSuccessfulEmailDate &&
                  emailDate.getTime() > lastSuccessfulEmailDate.getTime();

                if (!isNewerEmail) {
                  result.logs.push({
                    emailFrom: from,
                    emailSubject: subject,
                    fileName: attachment.filename,
                    status: "skipped",
                    error:
                      "File already processed (duplicate hash, same or older email date)",
                  });
                  continue;
                }

                console.log(
                  `[Email Fetcher] Allowing reprocess of ${attachment.filename} - email date ${emailDate?.toISOString()} is newer than last success ${lastSuccessfulEmailDate?.toISOString()}`,
                );
              }

              try {
                const importResult = await processEmailAttachment(
                  dataSourceId,
                  attachment.content,
                  attachment.filename,
                  forceStageMultiple, // FIX: Force staging when multiple files detected
                );

                await storage.createEmailFetchLog({
                  dataSourceId,
                  emailFrom: from,
                  emailSubject: subject,
                  emailDate: emailDate || null,
                  fileName: attachment.filename,
                  fileHash,
                  rowCount: importResult.rowCount,
                  status: importResult.success ? "success" : "error",
                  errorMessage: importResult.error || null,
                });

                if (importResult.success) {
                  result.filesProcessed++;
                  result.logs.push({
                    emailFrom: from,
                    emailSubject: subject,
                    fileName: attachment.filename,
                    status: "success",
                  });
                  // Update lastSuccessfulEmailDate so subsequent duplicates in same run are skipped
                  if (emailDate) {
                    lastSuccessfulEmailDate = emailDate;
                  }

                  if (importResult.staged) {
                    // Multi-file mode: file staged for later combine - don't trigger sync yet
                    anyFileStaged = true;
                  } else {
                    // Single-file mode: file imported directly - trigger consolidation
                    anyFileImported = true;

                    // Trigger auto-consolidation (matching manual upload behavior)
                    // NOTE: Consolidation runs per-file which is correct behavior
                    try {
                      await triggerAutoConsolidationAfterImport(dataSourceId);
                    } catch (err: any) {
                      console.error(
                        "Error in auto-consolidation after email import:",
                        err.message,
                      );
                    }
                  }

                  // NOTE: Sync is now deferred until ALL attachments are processed
                  // This prevents partial data sync when email has multiple files
                } else {
                  result.logs.push({
                    emailFrom: from,
                    emailSubject: subject,
                    fileName: attachment.filename,
                    status: "error",
                    error: importResult.error,
                  });
                  result.errors.push(
                    `Failed to import ${attachment.filename}: ${importResult.error}`,
                  );
                }
              } catch (err: any) {
                result.logs.push({
                  emailFrom: from,
                  emailSubject: subject,
                  fileName: attachment.filename,
                  status: "error",
                  error: err.message,
                });
                result.errors.push(
                  `Failed to process ${attachment.filename}: ${err.message}`,
                );
              }
            }

            // CRITICAL FIX: Trigger Shopify sync AFTER all attachments are processed
            // This prevents partial data sync when email contains multiple inventory files
            if (anyFileImported) {
              successfulUids.add(uid);
              console.log(
                `[Email Fetcher] All attachments processed for ${from}, triggering Shopify sync...`,
              );
              triggerShopifySyncAfterImport(dataSourceId).catch((err: any) => {
                console.error(
                  "Error triggering Shopify sync after email import:",
                  err.message,
                );
              });
            }
            // Multi-file staged: mark email as processed but defer sync to combine step
            if (anyFileStaged) {
              successfulUids.add(uid);
              anyFileStagedGlobal = true;
              console.log(
                `[Email Fetcher] ${result.filesProcessed} file(s) staged for ${from}, sync deferred to combine step`,
              );
            }
          }

          if (settings.markAsRead) {
            await client.messageFlagsAdd(uid, ["\\Seen"], { uid: true });
          }
        } catch (msgErr: any) {
          result.errors.push(
            `Error processing message UID ${uid}: ${msgErr.message}`,
          );
        }
      }

      // FIX: After all emails are processed, trigger combine for multi-file sources
      // Previously this was only handled by the scheduler, leaving staged files sitting indefinitely
      if (anyFileStagedGlobal) {
        console.log(
          `[Email Fetcher] All emails processed, triggering combine for data source ${dataSourceId}...`,
        );
        try {
          const combineResult = await combineAndImportStagedFiles(dataSourceId);
          if (combineResult.success) {
            console.log(
              `[Email Fetcher] Combine successful: ${combineResult.rowCount} items imported`,
            );
            // Trigger consolidation and sync after successful combine
            try {
              await triggerAutoConsolidationAfterImport(dataSourceId);
            } catch (err: any) {
              console.error(
                "Error in auto-consolidation after combine:",
                err.message,
              );
            }
            triggerShopifySyncAfterImport(dataSourceId).catch((err: any) => {
              console.error(
                "Error triggering Shopify sync after combine:",
                err.message,
              );
            });
          } else {
            console.error(
              `[Email Fetcher] Combine failed: ${combineResult.error}`,
            );
          }
        } catch (combineErr: any) {
          console.error(
            `[Email Fetcher] Error in combine step: ${combineErr.message}`,
          );
        }
      }

      // Send email notification ONCE after all emails are processed
      // Only for single-sender mode - multi-sender mode notifications are handled by scheduler after combine
      // Note: multiFileMode is typically undefined for single-sender email sources
      const isMultiSender =
        settings.senderWhitelist && settings.senderWhitelist.length > 1;
      if (result.filesProcessed > 0 && !isMultiSender) {
        storage
          .getDataSource(dataSourceId)
          .then((dataSource) => {
            if (dataSource) {
              sendImportSuccessNotification({
                dataSourceName: dataSource.name,
                importType: "email",
                itemsImported: result.filesProcessed,
                itemsSkipped: 0,
                durationSeconds: 0,
              }).catch((err: any) => {
                console.error(
                  "Error sending import success notification:",
                  err.message,
                );
              });
            }
          })
          .catch((err: any) => {
            console.error(
              "Error fetching data source for notification:",
              err.message,
            );
          });
      }

      if (settings.deleteAfterDownload && result.filesProcessed > 0) {
        try {
          for (const msgInfo of allWhitelistedMessages.filter((m) =>
            successfulUids.has(m.uid),
          )) {
            await client.messageFlagsAdd(msgInfo.uid, ["\\Deleted"], {
              uid: true,
            });
            console.log(
              `[Email Fetcher] Marked email UID ${msgInfo.uid} for deletion`,
            );
          }
          // noop() does NOT expunge - deleted flags are expunged on client.logout() below
          console.log(
            `[Email Fetcher] Emails flagged \\Deleted; expunge will occur on logout`,
          );
        } catch (expungeErr: any) {
          console.error(
            `[Email Fetcher] Failed to delete emails: ${expungeErr.message}`,
          );
        }
      }
    } finally {
      lock.release();
    }

    await client.logout();
  } catch (err: any) {
    result.success = false;
    result.errors.push(`IMAP connection error: ${err.message}`);
  }

  return result;
}

async function extractAttachments(
  client: ImapFlow,
  uid: number,
): Promise<Array<{ filename: string; content: Buffer }>> {
  const attachments: Array<{ filename: string; content: Buffer }> = [];

  try {
    const message = await client.fetchOne(
      uid,
      { bodyStructure: true },
      { uid: true },
    );

    if (!message) return attachments;

    const bodyStructure = (message as any).bodyStructure;
    if (!bodyStructure) {
      console.log(`[Email Fetcher] No body structure for UID ${uid}`);
      return attachments;
    }

    const parts = flattenParts(bodyStructure);
    console.log(
      `[Email Fetcher] Found ${parts.length} parts in email UID ${uid}`,
    );

    for (const part of parts) {
      // Get filename from multiple possible locations
      const filename =
        part.dispositionParameters?.filename ||
        part.parameters?.name ||
        (part.type === "application" && part.subtype
          ? `attachment.${part.subtype}`
          : null);

      // Check for attachments - can be disposition=attachment, inline, or just have a filename
      const isAttachment =
        part.disposition === "attachment" ||
        part.disposition === "inline" ||
        (filename && (part.type === "application" || part.encoding));

      if (filename) {
        console.log(
          `[Email Fetcher] Found part: ${filename}, disposition: ${part.disposition}, type: ${part.type}/${part.subtype}`,
        );
      }

      if (isAttachment && filename && isExcelFile(filename)) {
        try {
          console.log(
            `[Email Fetcher] Downloading Excel attachment: ${filename}`,
          );
          const { content } = await client.download(uid.toString(), part.part, {
            uid: true,
          });

          const chunks: Buffer[] = [];
          for await (const chunk of content) {
            chunks.push(chunk);
          }

          attachments.push({
            filename,
            content: Buffer.concat(chunks),
          });
          console.log(`[Email Fetcher] Successfully downloaded: ${filename}`);
        } catch (dlErr) {
          console.error(`Failed to download attachment ${filename}:`, dlErr);
        }
      }
    }
  } catch (err) {
    console.error("Error extracting attachments:", err);
  }

  return attachments;
}

function flattenParts(structure: any, parentPart = ""): any[] {
  const parts: any[] = [];

  if (!structure) return parts;

  if (structure.childNodes && Array.isArray(structure.childNodes)) {
    structure.childNodes.forEach((child: any, index: number) => {
      const partNum = parentPart
        ? `${parentPart}.${index + 1}`
        : `${index + 1}`;
      child.part = partNum;
      parts.push(child);
      parts.push(...flattenParts(child, partNum));
    });
  } else {
    structure.part = parentPart || "1";
    parts.push(structure);
  }

  return parts;
}

async function extractLinksFromEmailBody(
  client: ImapFlow,
  uid: number,
): Promise<Array<{ filename: string; content: Buffer }>> {
  const files: Array<{ filename: string; content: Buffer }> = [];

  try {
    const message = await client.fetchOne(
      uid,
      { bodyStructure: true },
      { uid: true },
    );

    if (!message) return files;

    const bodyStructure = (message as any).bodyStructure;
    if (!bodyStructure) return files;

    const parts = flattenParts(bodyStructure);

    let htmlContent = "";
    let textContent = "";

    for (const part of parts) {
      if (
        part.type === "text" &&
        (part.subtype === "html" || part.subtype === "plain")
      ) {
        try {
          const { content } = await client.download(uid.toString(), part.part, {
            uid: true,
          });
          const chunks: Buffer[] = [];
          for await (const chunk of content) {
            chunks.push(chunk);
          }
          const decoded = Buffer.concat(chunks).toString("utf-8");
          if (part.subtype === "html") {
            htmlContent = decoded;
          } else {
            textContent = decoded;
          }
        } catch (e) {
          console.error(
            `[Email Fetcher] Failed to get body part ${part.part}:`,
            e,
          );
        }
      }
    }

    const bodyContent = htmlContent || textContent;
    if (!bodyContent) {
      console.log(`[Email Fetcher] No body content found for UID ${uid}`);
      return files;
    }

    const urlRegex = /https?:\/\/[^\s"'<>]+\.(?:csv|xlsx|xls)/gi;
    const hrefRegex =
      /href=["']([^"']+(?:\.csv|\.xlsx|\.xls|_xt=\.csv)[^"']*)/gi;
    const netsuiteRegex =
      /https?:\/\/[^\s"'<>]+netsuite\.com[^\s"'<>]+_xt=\.csv[^\s"'<>]*/gi;

    const urls = new Set<string>();

    let match;
    while ((match = urlRegex.exec(bodyContent)) !== null) {
      let url = match[0];
      url = url.replace(/&amp;/g, "&");
      urls.add(url);
    }
    while ((match = hrefRegex.exec(bodyContent)) !== null) {
      let url = match[1];
      url = url.replace(/&amp;/g, "&");
      urls.add(url);
    }
    while ((match = netsuiteRegex.exec(bodyContent)) !== null) {
      let url = match[0];
      url = url.replace(/&amp;/g, "&");
      urls.add(url);
    }

    console.log(
      `[Email Fetcher] Found ${urls.size} download URLs in email body`,
    );

    const browserHeaders: Record<string, string> = {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      Accept:
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "Accept-Encoding": "gzip, deflate, br",
      Connection: "keep-alive",
      "Upgrade-Insecure-Requests": "1",
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
      "Sec-Fetch-User": "?1",
      "Cache-Control": "max-age=0",
    };

    for (const url of urls) {
      try {
        console.log(`[Email Fetcher] Link found: ${url}`);
        console.log(
          `[Email Fetcher] Step 1: Initial request to capture cookies for ${url.substring(0, 80)}...`,
        );

        // Step 1: Initial request to capture Akamai bot protection cookies
        const initialResponse = await fetch(url, {
          headers: browserHeaders,
          redirect: "manual",
        });

        console.log(
          `[Email Fetcher] Initial response: HTTP ${initialResponse.status} ${initialResponse.statusText}`,
        );

        // Extract cookies from Set-Cookie headers
        let cookies = "";
        const setCookieHeaders =
          initialResponse.headers.getSetCookie?.() || [];
        if (setCookieHeaders.length > 0) {
          cookies = setCookieHeaders
            .map((c: string) => c.split(";")[0])
            .join("; ");
          console.log(
            `[Email Fetcher] Captured ${setCookieHeaders.length} cookie(s) from initial response`,
          );
        }

        // If initial response was a redirect, capture the location too
        const redirectLocation = initialResponse.headers.get("location");
        const downloadUrl = redirectLocation || url;
        if (redirectLocation) {
          console.log(
            `[Email Fetcher] Following redirect to: ${redirectLocation.substring(0, 80)}...`,
          );
        }

        // Step 2: Download with captured cookies and full browser headers
        console.log(`[Email Fetcher] Step 2: Downloading with cookies...`);
        const downloadHeaders: Record<string, string> = {
          ...browserHeaders,
          Referer: new URL(url).origin + "/",
        };
        if (cookies) {
          downloadHeaders["Cookie"] = cookies;
        }

        let response = await fetch(downloadUrl, {
          headers: downloadHeaders,
          redirect: "follow",
        });

        console.log(
          `[Email Fetcher] Download response: HTTP ${response.status} ${response.statusText}`,
        );

        // Step 3: If still blocked (405/403), retry with accumulated cookies
        if (!response.ok && (response.status === 405 || response.status === 403)) {
          console.log(
            `[Email Fetcher] Step 3: Got ${response.status}, retrying with updated cookies...`,
          );
          const retrySetCookies =
            response.headers.getSetCookie?.() || [];
          if (retrySetCookies.length > 0) {
            const newCookies = retrySetCookies
              .map((c: string) => c.split(";")[0])
              .join("; ");
            cookies = cookies ? `${cookies}; ${newCookies}` : newCookies;
            console.log(
              `[Email Fetcher] Accumulated ${retrySetCookies.length} additional cookie(s)`,
            );
          }

          const retryHeaders: Record<string, string> = {
            ...browserHeaders,
            Referer: new URL(url).origin + "/",
          };
          if (cookies) {
            retryHeaders["Cookie"] = cookies;
          }

          response = await fetch(downloadUrl, {
            headers: retryHeaders,
            redirect: "follow",
          });

          console.log(
            `[Email Fetcher] Retry response: HTTP ${response.status} ${response.statusText}`,
          );
        }

        if (!response.ok) {
          console.error(
            `[Email Fetcher] FAILED to download: HTTP ${response.status} ${response.statusText} for ${url}`,
          );
          const respHeaders: Record<string, string> = {};
          response.headers.forEach((value, key) => {
            respHeaders[key] = value;
          });
          console.error(
            `[Email Fetcher] Response headers:`,
            JSON.stringify(respHeaders, null, 2),
          );
          continue;
        }

        const buffer = Buffer.from(await response.arrayBuffer());

        // Check if response is actually HTML (bot challenge page) instead of file data
        const contentType = response.headers.get("content-type") || "";
        if (
          contentType.includes("text/html") &&
          buffer.length < 50000 &&
          buffer.toString("utf-8").includes("<html")
        ) {
          console.error(
            `[Email Fetcher] BLOCKED: Response is an HTML page (likely bot challenge), not a file. URL: ${url}`,
          );
          console.error(
            `[Email Fetcher] Content-Type: ${contentType}, Size: ${buffer.length} bytes`,
          );
          continue;
        }

        let filename = "download.csv";
        const contentDisposition = response.headers.get("content-disposition");
        if (contentDisposition) {
          const filenameMatch = contentDisposition.match(
            /filename[*]?=["']?([^;"'\n]+)/i,
          );
          if (filenameMatch) {
            filename = filenameMatch[1].trim();
          }
        }

        if (filename === "download.csv") {
          const urlParts = url.split("/");
          const lastPart = urlParts[urlParts.length - 1];
          const pathPart = lastPart.split("?")[0];
          if (isExcelFile(pathPart)) {
            filename = pathPart;
          } else {
            const timestamp = Date.now();
            filename = `email_link_${timestamp}.csv`;
          }
        }

        console.log(
          `[Email Fetcher] SUCCESS: Downloaded ${filename} (${buffer.length} bytes) from ${url.substring(0, 80)}`,
        );
        files.push({ filename, content: buffer });
      } catch (dlErr: any) {
        console.error(
          `[Email Fetcher] EXCEPTION downloading from ${url}: ${dlErr.message}`,
        );
        console.error(`[Email Fetcher] Stack trace:`, dlErr.stack);
      }
    }
  } catch (err) {
    console.error("[Email Fetcher] Error extracting links from body:", err);
  }

  return files;
}

export async function testEmailConnection(
  settings: EmailSettings,
): Promise<{ success: boolean; error?: string; folderCount?: number }> {
  if (!settings.host || !settings.username || !settings.password) {
    return { success: false, error: "Missing email configuration" };
  }

  const client = new ImapFlow({
    host: settings.host,
    port: settings.port || 993,
    secure: settings.secure !== false,
    auth: {
      user: settings.username,
      pass: settings.password,
    },
    logger: false,
    socketTimeout: 15000,
  });

  client.on("error", (err: Error) => {
    console.error(
      `[Email Test] IMAP client error for ${settings.host}:`,
      err.message,
    );
  });

  try {
    await client.connect();

    const folders = await client.list();

    await client.logout();

    return { success: true, folderCount: folders.length };
  } catch (err: any) {
    return { success: false, error: err.message };
  }
}
