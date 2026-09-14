interface FriendCacheEntry {
  friendIds: string[];
  expiresAt: number;
}

export interface SocialDirectoryOptions {
  baseUrl?: string;
  friendsPath?: string;
  token?: string;
  ttlMs?: number;
  timeoutMs?: number;
}

export class SocialDirectory {
  private readonly cache = new Map<string, FriendCacheEntry>();
  private readonly options: Required<SocialDirectoryOptions>;

  public constructor(options: SocialDirectoryOptions = {}) {
    this.options = {
      baseUrl: options.baseUrl ?? "",
      friendsPath: options.friendsPath ?? "/api/internal/friends/{playerId}",
      token: options.token ?? "",
      ttlMs: options.ttlMs ?? 60_000,
      timeoutMs: options.timeoutMs ?? 1_000
    };
  }

  public async getFriendIds(playerId: string, forceRefresh = false): Promise<string[]> {
    const key = String(playerId);
    const cached = this.cache.get(key);
    if (!forceRefresh && cached && cached.expiresAt > Date.now()) return [...cached.friendIds];
    if (!this.options.baseUrl) return cached ? [...cached.friendIds] : [];
    try {
      const path = this.options.friendsPath.replace("{playerId}", encodeURIComponent(key));
      const url = new URL(path, this.options.baseUrl).toString();
      const headers: Record<string, string> = { accept: "application/json" };
      if (this.options.token) headers.authorization = `Bearer ${this.options.token}`;
      const response = await fetch(url, { headers, signal: AbortSignal.timeout(this.options.timeoutMs) });
      if (!response.ok) throw new Error(`friends api returned ${response.status}`);
      const payload = await response.json() as unknown;
      const friendIds = normalizeFriendIds(payload);
      this.cache.set(key, { friendIds, expiresAt: Date.now() + this.options.ttlMs });
      return [...friendIds];
    } catch {
      return cached ? [...cached.friendIds] : [];
    }
  }

  public setFriendIds(playerId: string, friendIds: string[]): void {
    const normalized = [...new Set(friendIds.map(String).filter(Boolean))];
    this.cache.set(String(playerId), { friendIds: normalized, expiresAt: Date.now() + this.options.ttlMs });
  }
}

function normalizeFriendIds(payload: unknown): string[] {
  const root = payload && typeof payload === "object" ? payload as Record<string, unknown> : {};
  const data = root.data && typeof root.data === "object" ? root.data as Record<string, unknown> : root;
  const friends = Array.isArray(data.friendIds) ? data.friendIds : Array.isArray(data.friends) ? data.friends : [];
  return [...new Set(friends.map((friend) => {
    if (typeof friend === "string" || typeof friend === "number") return String(friend);
    if (friend && typeof friend === "object") {
      const value = friend as Record<string, unknown>;
      return String(value.playerId ?? value.userId ?? value.id ?? "");
    }
    return "";
  }).filter(Boolean))];
}
