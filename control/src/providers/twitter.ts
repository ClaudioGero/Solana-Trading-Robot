const X_API_BASE_URL = process.env.X_API_BASE_URL ?? "https://api.twitter.com/2";
const TWITTERAPI_IO_BASE_URL = process.env.TWITTERAPI_IO_BASE_URL ?? "https://api.twitterapi.io/twitter/community";

export type TwitterAuthorProfile = {
  id: string;
  username: string;
  name: string;
  verified: boolean;
  followers: number | null;
  following: number | null;
  tweetCount: number | null;
  listedCount: number | null;
  createdAt: string | null;
  description: string | null;
  relevanceScore: number;
};

export type TwitterPost = {
  id: string;
  text: string;
  createdAt: string | null;
  likeCount: number;
  repostCount: number;
  replyCount: number;
  quoteCount: number;
  author: TwitterAuthorProfile | null;
};

export type TwitterSearchBucket = {
  query: string;
  postCount: number;
  authorCount: number;
  filteredOutPostCount: number;
  posts: TwitterPost[];
  topAuthors: TwitterAuthorProfile[];
  engagementTotals: {
    likeCount: number;
    repostCount: number;
    replyCount: number;
    quoteCount: number;
  };
};

export type TwitterLinkedAccount = {
  source: "birdeye" | "dexscreener";
  url: string;
  username: string | null;
  kind: "official_or_project" | "community" | "unknown";
  communityId?: string | null;
  profile: TwitterAuthorProfile | null;
  recentPosts: TwitterPost[];
  note: string | null;
};

export type TwitterCommunityUser = {
  id: string;
  username: string;
  name: string;
  verified: boolean;
  followers: number | null;
  following: number | null;
  tweetCount: number | null;
  listedCount: number | null;
  description: string | null;
  createdAt: string | null;
  relevanceScore: number;
};

export type TwitterCommunitySignal = {
  communityId: string;
  url: string;
  tweets: TwitterPost[];
  moderators: TwitterCommunityUser[];
  verifiedMembers: TwitterCommunityUser[];
  errors: Partial<Record<"tweets" | "moderators" | "members", string>>;
};

export type TwitterTokenSignal = {
  linkedAccounts: TwitterLinkedAccount[];
  community: TwitterCommunitySignal | null;
  contractSearch: TwitterSearchBucket;
  tickerSearch: TwitterSearchBucket | null;
  summary: {
    linkedAccountCount: number;
    communityTweetCount: number;
    communityModeratorCount: number;
    communityVerifiedMemberCount: number;
    contractPostCount: number;
    tickerPostCount: number;
    filteredOutPostCount: number;
    totalRelevantPostCount: number;
  };
};

type CommunityApiUser = {
  id?: string;
  userName?: string;
  name?: string;
  isBlueVerified?: boolean;
  verifiedType?: string;
  description?: string;
  followers?: number;
  createdAt?: string;
};

type CommunityApiTweet = {
  id?: string;
  text?: string;
  createdAt?: string;
  likeCount?: number;
  retweetCount?: number;
  replyCount?: number;
  quoteCount?: number;
  author?: CommunityApiUser;
};

const SCAMMY_PHRASES = [
  "airdrop",
  "collab",
  "claim now",
  "first movers",
  "moonshot",
  "vote now",
];

type SearchRecentResponse = {
  data?: Array<{
    id: string;
    author_id?: string;
    text?: string;
    created_at?: string;
    public_metrics?: {
      like_count?: number;
      retweet_count?: number;
      reply_count?: number;
      quote_count?: number;
    };
  }>;
  includes?: {
    users?: Array<{
      id: string;
      username?: string;
      name?: string;
      verified?: boolean;
      description?: string;
      created_at?: string;
      public_metrics?: {
        followers_count?: number;
        following_count?: number;
        tweet_count?: number;
        listed_count?: number;
      };
    }>;
  };
};

export async function getTwitterTokenSignal(input: {
  tokenAddress: string;
  symbol?: string | null;
  name?: string | null;
  linkedAccounts?: Array<{
    source: "birdeye" | "dexscreener";
    url: string;
  }>;
}): Promise<TwitterTokenSignal> {
  ensureBearerToken();

  const linkedAccounts = await getLinkedAccountAnalyses(input.linkedAccounts ?? [], input.symbol, input.name);
  const linkedCommunity = linkedAccounts.find(
    (account) => account.kind === "community" && account.communityId,
  );
  const community = linkedCommunity?.communityId
    ? await getCommunitySignal(linkedCommunity.communityId, linkedCommunity.url, input.symbol, input.name)
    : null;
  const contractSearch = await searchRecentTweets({
    query: buildContractQuery(input.tokenAddress),
    symbol: input.symbol,
    name: input.name,
    maxPosts: 12,
    maxAuthors: 10,
    sourceKind: "contract",
  });

  const tickerSearch = input.symbol && !community
    ? await searchRecentTweets({
        query: buildTickerQuery(input.symbol),
        symbol: input.symbol,
        name: input.name,
        maxPosts: 3,
        maxAuthors: 5,
        sourceKind: "ticker",
      })
    : null;

  return {
    linkedAccounts,
    community,
    contractSearch,
    tickerSearch,
    summary: {
      linkedAccountCount: linkedAccounts.length,
      communityTweetCount: community?.tweets.length ?? 0,
      communityModeratorCount: community?.moderators.length ?? 0,
      communityVerifiedMemberCount: community?.verifiedMembers.length ?? 0,
      contractPostCount: contractSearch.postCount,
      tickerPostCount: tickerSearch?.postCount ?? 0,
      filteredOutPostCount:
        contractSearch.filteredOutPostCount + (tickerSearch?.filteredOutPostCount ?? 0),
      totalRelevantPostCount:
        (community?.tweets.length ?? 0) + contractSearch.postCount + (tickerSearch?.postCount ?? 0),
    },
  };
}

async function getLinkedAccountAnalyses(
  inputs: Array<{
    source: "birdeye" | "dexscreener";
    url: string;
  }>,
  symbol?: string | null,
  name?: string | null,
): Promise<TwitterLinkedAccount[]> {
  const seen = new Set<string>();
  const uniqueInputs = inputs.filter((entry) => {
    const normalized = entry.url.trim();
    if (!normalized || seen.has(normalized)) {
      return false;
    }
    seen.add(normalized);
    return true;
  });

  return mapWithConcurrency(uniqueInputs, 3, async (entry) => {
    const parsed = parseTwitterUrl(entry.url);
    if (!parsed.username) {
      return {
        source: entry.source,
        url: entry.url,
        username: null,
        kind: parsed.kind,
        communityId: parsed.communityId,
        profile: null,
        recentPosts: [],
        note: parsed.note,
      };
    }

    let bucket: TwitterSearchBucket;
    try {
      bucket = await searchRecentTweets({
        query: `from:${parsed.username} -is:retweet`,
        symbol,
        name,
        maxPosts: 5,
        maxAuthors: 1,
        sourceKind: "linked",
      });
    } catch (error) {
      return {
        source: entry.source,
        url: entry.url,
        username: parsed.username,
        kind: parsed.kind,
        profile: null,
        recentPosts: [],
        note: error instanceof Error ? error.message : "failed fetching linked account posts",
      };
    }

    return {
      source: entry.source,
      url: entry.url,
      username: parsed.username,
      kind: parsed.kind,
      communityId: parsed.communityId,
      profile: bucket.topAuthors[0] ?? null,
      recentPosts: bucket.posts,
      note: parsed.note,
    };
  });
}

async function searchRecentTweets(input: {
  query: string;
  symbol?: string | null;
  name?: string | null;
  maxPosts: number;
  maxAuthors: number;
  sourceKind: "linked" | "contract" | "ticker";
}): Promise<TwitterSearchBucket> {
  const response = await fetch(buildSearchUrl(input.query, input.maxPosts), {
    headers: {
      authorization: `Bearer ${ensureBearerToken()}`,
      accept: "application/json",
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`X recent search failed: status=${response.status} body=${body}`);
  }

  const payload = (await response.json()) as SearchRecentResponse;
  const allAuthors = (payload.includes?.users ?? [])
    .map<TwitterAuthorProfile>((user) => ({
      id: user.id ?? "",
      username: user.username ?? "",
      name: user.name ?? "",
      verified: Boolean(user.verified),
      followers: user.public_metrics?.followers_count ?? null,
      following: user.public_metrics?.following_count ?? null,
      tweetCount: user.public_metrics?.tweet_count ?? null,
      listedCount: user.public_metrics?.listed_count ?? null,
      createdAt: user.created_at ?? null,
      description: user.description ?? null,
      relevanceScore: scoreAuthor(user, input.symbol, input.name),
    }));

  const authorById = new Map(allAuthors.map((author) => [author.id, author]));
  const rawPosts = (payload.data ?? [])
    .map<TwitterPost>((post) => ({
      id: post.id,
      text: post.text ?? "",
      createdAt: post.created_at ?? null,
      likeCount: post.public_metrics?.like_count ?? 0,
      repostCount: post.public_metrics?.retweet_count ?? 0,
      replyCount: post.public_metrics?.reply_count ?? 0,
      quoteCount: post.public_metrics?.quote_count ?? 0,
      author: post.author_id ? authorById.get(post.author_id) ?? null : null,
    }));
  const posts = rawPosts
    .filter((post) => shouldKeepPost(post, input.sourceKind))
    .slice(0, input.maxPosts);
  const filteredOutPostCount = Math.max(rawPosts.length - posts.length, 0);
  const keptAuthorIds = new Set(
    posts
      .map((post) => post.author?.id)
      .filter((authorId): authorId is string => Boolean(authorId)),
  );
  const authors = allAuthors
    .filter((author) => keptAuthorIds.has(author.id))
    .sort((left, right) => right.relevanceScore - left.relevanceScore)
    .slice(0, input.maxAuthors);

  return {
    query: input.query,
    postCount: posts.length,
    authorCount: authors.length,
    filteredOutPostCount,
    posts,
    topAuthors: authors,
    engagementTotals: {
      likeCount: posts.reduce((sum, post) => sum + post.likeCount, 0),
      repostCount: posts.reduce((sum, post) => sum + post.repostCount, 0),
      replyCount: posts.reduce((sum, post) => sum + post.replyCount, 0),
      quoteCount: posts.reduce((sum, post) => sum + post.quoteCount, 0),
    },
  };
}

function buildSearchUrl(query: string, maxPosts: number): URL {
  const url = new URL(`${X_API_BASE_URL}/tweets/search/recent`);
  url.searchParams.set("query", query);
  url.searchParams.set("max_results", String(clamp(maxPosts, 10, 15)));
  url.searchParams.set("expansions", "author_id");
  url.searchParams.set("tweet.fields", "created_at,public_metrics,lang,author_id");
  url.searchParams.set("user.fields", "created_at,description,public_metrics,verified");
  return url;
}

function buildContractQuery(tokenAddress: string): string {
  return `"${tokenAddress}" -is:retweet`;
}

function buildTickerQuery(symbol: string): string {
  const cleanSymbol = symbol.trim().replace(/^\$/, "");
  return `"$${cleanSymbol}" -is:retweet`;
}

function parseTwitterUrl(url: string): {
  username: string | null;
  kind: "official_or_project" | "community" | "unknown";
  communityId: string | null;
  note: string | null;
} {
  try {
    const parsed = new URL(url);
    const host = parsed.hostname.replace(/^www\./, "");
    if (host !== "x.com" && host !== "twitter.com") {
      return { username: null, kind: "unknown", communityId: null, note: "non-x url" };
    }

    const pathSegments = parsed.pathname.split("/").filter(Boolean);
    if (pathSegments.length === 0) {
      return { username: null, kind: "unknown", communityId: null, note: "missing username in x url" };
    }

    if (pathSegments[0] === "i" && pathSegments[1] === "communities") {
      return {
        username: null,
        kind: "community",
        communityId: pathSegments[2] ?? null,
        note: pathSegments[2] ? null : "x community url missing community id",
      };
    }

    const username = pathSegments[0];
    if (!username || ["home", "search", "explore", "i", "share"].includes(username.toLowerCase())) {
      return {
        username: null,
        kind: "unknown",
        communityId: null,
        note: "x url does not point to a concrete account",
      };
    }

    return {
      username,
      kind: "official_or_project",
      communityId: null,
      note: null,
    };
  } catch {
    return { username: null, kind: "unknown", communityId: null, note: "invalid x url" };
  }
}

function shouldKeepPost(
  post: TwitterPost,
  sourceKind: "linked" | "contract" | "ticker",
): boolean {
  if (sourceKind === "linked") {
    return true;
  }

  const author = post.author;
  const followers = author?.followers ?? 0;
  if (!author?.username?.trim() || !author.name?.trim()) {
    return false;
  }
  if (followers < 100) {
    return false;
  }

  const normalizedText = post.text.toLowerCase();
  if (containsScammyPhrase(normalizedText)) {
    return false;
  }
  if (containsExternalLink(normalizedText)) {
    return false;
  }
  if (sourceKind === "ticker" && !hasTickerSpecificContext(normalizedText, post, author)) {
    return false;
  }

  return true;
}

function containsScammyPhrase(text: string): boolean {
  return SCAMMY_PHRASES.some((phrase) => text.includes(phrase));
}

function containsExternalLink(text: string): boolean {
  return /https?:\/\/|www\./i.test(text);
}

function hasTickerSpecificContext(
  normalizedText: string,
  post: TwitterPost,
  author: TwitterAuthorProfile | null,
): boolean {
  const authorUsername = author?.username?.toLowerCase() ?? "";
  const authorName = author?.name?.toLowerCase() ?? "";
  return (
    normalizedText.includes("pump") ||
    normalizedText.includes("solana") ||
    normalizedText.includes("ca:") ||
    normalizedText.includes("contract") ||
    normalizedText.includes("dexscreener") ||
    normalizedText.includes("birdeye") ||
    authorUsername.includes("intel") ||
    authorUsername.includes("calls") ||
    authorName.includes("intel") ||
    authorName.includes("calls") ||
    post.text.includes("$")
  );
}

async function getCommunitySignal(
  communityId: string,
  url: string,
  symbol?: string | null,
  name?: string | null,
): Promise<TwitterCommunitySignal> {
  const errors: Partial<Record<"tweets" | "moderators" | "members", string>> = {};

  const tweetsPayload = await fetchCommunityEndpoint("tweets", communityId).catch((error: unknown) => {
    errors.tweets = error instanceof Error ? error.message : String(error);
    return null;
  });
  const moderatorsPayload = await fetchCommunityEndpoint("moderators", communityId).catch((error: unknown) => {
    errors.moderators = error instanceof Error ? error.message : String(error);
    return null;
  });
  const membersPayload = await fetchCommunityEndpoint("members", communityId).catch((error: unknown) => {
    errors.members = error instanceof Error ? error.message : String(error);
    return null;
  });

  const tweets = (((tweetsPayload?.tweets as CommunityApiTweet[] | undefined) ?? []))
    .map((tweet) => mapCommunityTweet(tweet, symbol, name))
    .filter((tweet) => shouldKeepCommunityTweet(tweet));
  const moderators = (((moderatorsPayload?.members as CommunityApiUser[] | undefined) ?? []))
    .map((user) => mapCommunityUser(user, symbol, name))
    .filter((user) => Boolean(user.username));
  const verifiedMembers = (((membersPayload?.members as CommunityApiUser[] | undefined) ?? []))
    .map((user) => mapCommunityUser(user, symbol, name))
    .filter((user) => user.verified)
    .sort((left, right) => right.relevanceScore - left.relevanceScore)
    .slice(0, 10);

  return {
    communityId,
    url,
    tweets: tweets.slice(0, 10),
    moderators,
    verifiedMembers,
    errors,
  };
}

async function fetchCommunityEndpoint(
  endpoint: "tweets" | "members" | "moderators",
  communityId: string,
): Promise<Record<string, unknown>> {
  const apiKey = ensureTwitterApiIoKey();
  const url = new URL(`${TWITTERAPI_IO_BASE_URL}/${endpoint}`);
  url.searchParams.set("community_id", communityId);
  const response = await fetch(url, {
    headers: {
      "X-API-Key": apiKey,
      accept: "application/json",
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`twitterapi.io community ${endpoint} failed: status=${response.status} body=${body}`);
  }

  return (await response.json()) as Record<string, unknown>;
}

function mapCommunityTweet(
  tweet: CommunityApiTweet,
  symbol?: string | null,
  name?: string | null,
): TwitterPost {
  return {
    id: tweet.id ?? "",
    text: tweet.text ?? "",
    createdAt: tweet.createdAt ?? null,
    likeCount: tweet.likeCount ?? 0,
    repostCount: tweet.retweetCount ?? 0,
    replyCount: tweet.replyCount ?? 0,
    quoteCount: tweet.quoteCount ?? 0,
    author: tweet.author ? mapCommunityUser(tweet.author, symbol, name) : null,
  };
}

function mapCommunityUser(
  user: CommunityApiUser,
  symbol?: string | null,
  name?: string | null,
): TwitterCommunityUser {
  const followers = user.followers ?? null;
  const profile = {
    username: user.userName ?? "",
    name: user.name ?? "",
    verified: Boolean(user.isBlueVerified || user.verifiedType),
    description: user.description ?? undefined,
    public_metrics: {
      followers_count: followers ?? undefined,
    },
  };

  return {
    id: user.id ?? "",
    username: user.userName ?? "",
    name: user.name ?? "",
    verified: Boolean(user.isBlueVerified || user.verifiedType),
    followers,
    following: null,
    tweetCount: null,
    listedCount: null,
    description: user.description ?? null,
    createdAt: user.createdAt ?? null,
    relevanceScore: scoreAuthor(profile, symbol, name),
  };
}

function shouldKeepCommunityTweet(tweet: TwitterPost): boolean {
  if (!tweet.author?.username?.trim() || !tweet.author.name?.trim()) {
    return false;
  }
  if ((tweet.author.followers ?? 0) < 100) {
    return false;
  }
  const normalizedText = tweet.text.toLowerCase();
  if (containsScammyPhrase(normalizedText)) {
    return false;
  }
  return true;
}

function scoreAuthor(
  user: {
    username?: string;
    name?: string;
    verified?: boolean;
    description?: string;
    public_metrics?: {
      followers_count?: number;
    };
  },
  symbol?: string | null,
  name?: string | null,
): number {
  const haystacks = [user.username, user.name, user.description]
    .filter((value): value is string => Boolean(value))
    .map((value) => value.toLowerCase());

  let score = 0;
  if (user.verified) {
    score += 2;
  }
  const followers = user.public_metrics?.followers_count ?? 0;
  if (followers >= 2_000 && followers <= 20_000) {
    score += 4;
  } else if (followers >= 500 && followers <= 70_000) {
    score += 3;
  } else if (followers >= 150) {
    score += 1;
  } else if (followers < 150) {
    score -= 2;
  }
  if (followers > 70_000) {
    score -= 1;
  }

  const normalizedSymbol = symbol?.trim().replace(/^\$/, "").toLowerCase();
  const normalizedName = name?.trim().toLowerCase();
  if (normalizedSymbol && haystacks.some((value) => value.includes(normalizedSymbol))) {
    score += 2;
  }
  if (normalizedName && haystacks.some((value) => value.includes(normalizedName))) {
    score += 1;
  }

  return score;
}

function ensureBearerToken(): string {
  const bearerToken = process.env.X_BEARER_TOKEN ?? process.env.TWITTER_BEARER_TOKEN;
  if (!bearerToken) {
    throw new Error("X_BEARER_TOKEN is not configured");
  }

  return bearerToken;
}

function ensureTwitterApiIoKey(): string {
  const apiKey = process.env.TWITTERAPI_IO_KEY;
  if (!apiKey) {
    throw new Error("TWITTERAPI_IO_KEY is not configured");
  }

  return apiKey;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max);
}

async function mapWithConcurrency<T, R>(
  items: T[],
  concurrency: number,
  mapFn: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let currentIndex = 0;

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (currentIndex < items.length) {
      const itemIndex = currentIndex;
      currentIndex += 1;
      results[itemIndex] = await mapFn(items[itemIndex], itemIndex);
    }
  });

  await Promise.all(workers);
  return results;
}
