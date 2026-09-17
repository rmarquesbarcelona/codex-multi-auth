import { request } from "node:http";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AccountManager } from "../lib/accounts.js";
import { startRuntimeRotationProxy, type RuntimeRotationProxyServer } from "../lib/runtime-rotation-proxy.js";
import { clearCircuitBreakers } from "../lib/circuit-breaker.js";
import { resetRefreshQueue } from "../lib/refresh-queue.js";
import { resetTrackers } from "../lib/rotation.js";
import { __resetRoutingMutexForTests } from "../lib/routing-mutex.js";
import type { AccountStorageV3 } from "../lib/storage.js";

const CLIENT_KEY = "runtime-image-lifecycle-test";
const openServers: RuntimeRotationProxyServer[] = [];
const openManagers: AccountManager[] = [];

const { saveAccountsMock, withAccountStorageTransactionMock } = vi.hoisted(() => ({
	saveAccountsMock: vi.fn(),
	withAccountStorageTransactionMock: vi.fn(),
}));

vi.mock("../lib/storage.js", async (importOriginal) => {
	const actual = await importOriginal<typeof import("../lib/storage.js")>();
	return {
		...actual,
		saveAccounts: saveAccountsMock,
		withAccountStorageTransaction: withAccountStorageTransactionMock,
	};
});

function createStorage(now: number): AccountStorageV3 {
	return {
		version: 3,
		activeIndex: 0,
		activeIndexByFamily: { codex: 0 },
		accounts: [
			{
				email: "image-lifecycle@example.com",
				accountId: "acc_image_lifecycle",
				refreshToken: "refresh-image-lifecycle",
				accessToken: "access-image-lifecycle",
				expiresAt: now + 3_600_000,
				addedAt: now - 60_000,
				lastUsed: now - 60_000,
				enabled: true,
			},
		],
	};
}

async function startProxy(fetchImpl: typeof fetch): Promise<RuntimeRotationProxyServer> {
	const manager = new AccountManager(undefined, createStorage(Date.now()));
	openManagers.push(manager);
	const proxy = await startRuntimeRotationProxy({
		accountManager: manager,
		fetchImpl,
		upstreamBaseUrl: "https://example.test/backend-api",
		clientApiKey: CLIENT_KEY,
		fetchTimeoutMs: 1,
		quotaRemainingPercentThreshold: 10,
	});
	openServers.push(proxy);
	return proxy;
}

function postJson(
	proxy: RuntimeRotationProxyServer,
	path: string,
	body: string,
): { response: Promise<{ status: number; body: string }>; destroy: () => void } {
	const url = new URL(`${proxy.baseUrl}${path}`);
	let clientRequest: ReturnType<typeof request>;
	const response = new Promise<{ status: number; body: string }>((resolve, reject) => {
		clientRequest = request(
			{
				host: url.hostname,
				port: Number(url.port),
				path: url.pathname,
				method: "POST",
				headers: {
					authorization: `Bearer ${CLIENT_KEY}`,
					"content-type": "application/json",
					"content-length": Buffer.byteLength(body).toString(),
				},
			},
			(res) => {
				const chunks: Buffer[] = [];
				res.on("data", (chunk) =>
					chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)),
				);
				res.on("end", () =>
					resolve({
						status: res.statusCode ?? 0,
						body: Buffer.concat(chunks).toString("utf8"),
					}),
				);
			},
		);
		clientRequest.on("error", reject);
		clientRequest.end(body);
	});
	return {
		response,
		destroy: () => {
			clientRequest.destroy();
		},
	};
}

function postImage(
	proxy: RuntimeRotationProxyServer,
	prompt: string,
): { response: Promise<{ status: number; body: string }>; destroy: () => void } {
	return postJson(
		proxy,
		"/images/generations",
		JSON.stringify({ model: "gpt-image-2", prompt }),
	);
}

function postResponses(
	proxy: RuntimeRotationProxyServer,
): { response: Promise<{ status: number; body: string }>; destroy: () => void } {
	return postJson(
		proxy,
		"/responses",
		JSON.stringify({ input: "disconnect", stream: false }),
	);
}

beforeEach(() => {
	resetTrackers();
	clearCircuitBreakers();
	resetRefreshQueue();
	__resetRoutingMutexForTests();
	saveAccountsMock.mockReset();
	saveAccountsMock.mockResolvedValue(undefined);
	withAccountStorageTransactionMock.mockReset();
	withAccountStorageTransactionMock.mockImplementation(async (handler) =>
		handler(null, async () => undefined),
	);
});

afterEach(async () => {
	vi.useRealTimers();
	vi.restoreAllMocks();
	for (const proxy of openServers.splice(0, openServers.length)) {
		await proxy.close();
	}
	for (const manager of openManagers.splice(0, openManagers.length)) {
		await manager.flushPendingSave();
	}
	resetTrackers();
	clearCircuitBreakers();
	resetRefreshQueue();
	__resetRoutingMutexForTests();
});

describe("image request lifecycle", () => {
	it("aborts the upstream fetch when the client disconnects before headers", async () => {
		let markStarted: (() => void) | undefined;
		const started = new Promise<void>((resolve) => {
			markStarted = resolve;
		});
		let markAborted: (() => void) | undefined;
		const aborted = new Promise<void>((resolve) => {
			markAborted = resolve;
		});
		const fetchImpl: typeof fetch = vi.fn(async (_input, init) => {
			markStarted?.();
			return await new Promise<Response>((_resolve, reject) => {
				const signal = init?.signal;
				if (!signal) {
					reject(new Error("expected fetch abort signal"));
					return;
				}
				const onAbort = () => {
					markAborted?.();
					reject(new DOMException("aborted", "AbortError"));
				};
				if (signal.aborted) onAbort();
				else signal.addEventListener("abort", onAbort, { once: true });
			});
		});
		const proxy = await startProxy(fetchImpl);
		const client = postImage(proxy, "disconnect");
		void client.response.catch(() => undefined);

		await started;
		client.destroy();
		await aborted;
		await new Promise<void>((resolve) => setImmediate(resolve));

		expect(fetchImpl).toHaveBeenCalledTimes(1);
	});

	it("keeps disconnect cancellation active after headers until streaming takes over", async () => {
		let fetchSignal: AbortSignal | undefined;
		const fetchImpl: typeof fetch = vi.fn(async (_input, init) => {
			fetchSignal = init?.signal ?? undefined;
			return new Response(null, {
				status: 200,
				headers: { "content-type": "application/json" },
			});
		});
		const proxy = await startProxy(fetchImpl);
		const manager = openManagers.at(-1);
		expect(manager).toBeDefined();

		let markPersistStarted: (() => void) | undefined;
		const persistStarted = new Promise<void>((resolve) => {
			markPersistStarted = resolve;
		});
		let releasePersist: (() => void) | undefined;
		const persistRelease = new Promise<void>((resolve) => {
			releasePersist = resolve;
		});
		vi.spyOn(manager!, "syncCodexCliActiveSelectionForIndex").mockImplementation(
			async () => {
				markPersistStarted?.();
				await persistRelease;
			},
		);

		const client = postImage(proxy, "handoff");
		void client.response.catch(() => undefined);
		await persistStarted;
		expect(fetchSignal?.aborted).toBe(false);

		client.destroy();
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(fetchSignal?.aborted).toBe(true);

		releasePersist?.();
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(fetchImpl).toHaveBeenCalledTimes(1);
	});

	it("does not abort or cool a responses request when the client disconnects before headers", async () => {
		let markStarted: (() => void) | undefined;
		const started = new Promise<void>((resolve) => {
			markStarted = resolve;
		});
		let settleUpstream: ((response: Response) => void) | undefined;
		let abortCount = 0;
		const fetchImpl: typeof fetch = vi.fn(async (_input, init) => {
			markStarted?.();
			return await new Promise<Response>((resolve, reject) => {
				settleUpstream = resolve;
				const signal = init?.signal;
				if (!signal) {
					reject(new Error("expected fetch abort signal"));
					return;
				}
				const onAbort = () => {
					abortCount += 1;
					reject(new DOMException("aborted", "AbortError"));
				};
				if (signal.aborted) onAbort();
				else signal.addEventListener("abort", onAbort, { once: true });
			});
		});
		const proxy = await startProxy(fetchImpl);
		const manager = openManagers.at(-1);
		expect(manager).toBeDefined();
		const client = postResponses(proxy);
		void client.response.catch(() => undefined);

		await started;
		client.destroy();
		await new Promise<void>((resolve) => setImmediate(resolve));

		expect(abortCount).toBe(0);
		expect(fetchImpl).toHaveBeenCalledTimes(1);
		const account = manager!.getAccountByIndex(0);
		expect(account).toBeDefined();
		expect(
			manager!.getManagedAccountRuntimeSkipReason(account!, "codex", null),
		).toBeNull();

		settleUpstream?.(
			new Response(null, {
				status: 200,
				headers: { "content-type": "application/json" },
			}),
		);
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(abortCount).toBe(0);
		expect(fetchImpl).toHaveBeenCalledTimes(1);
		expect(
			manager!.getManagedAccountRuntimeSkipReason(account!, "codex", null),
		).toBeNull();
	});

	it("keeps the image fetch pending through 299999 ms and returns one 502 at 300000 ms", async () => {
		let markStarted: (() => void) | undefined;
		const started = new Promise<void>((resolve) => {
			markStarted = resolve;
		});
		let abortCount = 0;
		const fetchImpl: typeof fetch = vi.fn(async (_input, init) => {
			markStarted?.();
			return await new Promise<Response>((_resolve, reject) => {
				const signal = init?.signal;
				if (!signal) {
					reject(new Error("expected fetch abort signal"));
					return;
				}
				const onAbort = () => {
					abortCount += 1;
					reject(new DOMException("aborted", "AbortError"));
				};
				if (signal.aborted) onAbort();
				else signal.addEventListener("abort", onAbort, { once: true });
			});
		});
		const proxy = await startProxy(fetchImpl);
		vi.useFakeTimers();
		const client = postImage(proxy, "timeout");
		let responseCount = 0;
		const responsePromise = client.response.then((value) => {
			responseCount += 1;
			return value;
		});
		await started;

		let settled = false;
		void responsePromise.finally(() => {
			settled = true;
		});
		await vi.advanceTimersByTimeAsync(299_999);
		await Promise.resolve();
		expect(settled).toBe(false);

		await vi.advanceTimersByTimeAsync(1);
		const response = await responsePromise;
		expect(response.status).toBe(502);
		expect(JSON.parse(response.body)).toMatchObject({
			error: { code: "image_upstream_transport_error" },
		});
		expect(responseCount).toBe(1);
		expect(abortCount).toBe(1);
		expect(fetchImpl).toHaveBeenCalledTimes(1);
	});
});
