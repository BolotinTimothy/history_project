const telegram = window.Telegram?.WebApp ?? null;
const appRoot = document.querySelector("#app");
const backButton = document.querySelector("#backButton");
const reloadButton = document.querySelector("#reloadButton");
const storyCardTemplate = document.querySelector("#storyCardTemplate");
const SHARE_BOT_HANDLE = "@StoryCompleterBot";
const SHARE_BOT_URL = "https://t.me/StoryCompleterBot";

const state = {
  user: null,
  stories: [],
  session: null,
  profile: null,
  feedback: null,
  completion: null,
  newAchievements: [],
  screen: "loading",
};

initTelegram();
backButton.addEventListener("click", handleBack);
reloadButton.addEventListener("click", loadState);
loadState();

function initTelegram() {
  applyTelegramTheme();

  if (!telegram) {
    return;
  }

  telegram.ready();
  telegram.expand();
  applyTelegramTheme();

  if (supportsTelegram("6.1")) {
    try {
      telegram.setHeaderColor?.("secondary_bg_color");
      telegram.setBackgroundColor?.(getTelegramBackgroundColor());
    } catch {
      // Telegram clients differ slightly by platform; visual hints are optional.
    }
  }

  telegram.onEvent?.("themeChanged", applyTelegramTheme);

  if (supportsTelegram("6.1")) {
    telegram.BackButton?.onClick(handleBack);
  }
}

function applyTelegramTheme() {
  const previewTheme = getPreviewThemeOverride();
  const systemThemeIsDark =
    telegram?.colorScheme === "dark" ||
    isDarkColor(telegram?.themeParams?.bg_color) ||
    isDarkColor(telegram?.themeParams?.secondary_bg_color) ||
    (!telegram && window.matchMedia?.("(prefers-color-scheme: dark)").matches);
  const isDark = previewTheme === "dark" || (!previewTheme && systemThemeIsDark);
  const theme = isDark ? "dark" : "light";
  document.documentElement.dataset.theme = theme;
  document.querySelector('meta[name="theme-color"]')?.setAttribute("content", getTelegramBackgroundColor(theme));

  if (supportsTelegram("6.1")) {
    try {
      telegram.setBackgroundColor?.(getTelegramBackgroundColor(theme));
    } catch {
      // Telegram theme sync is best-effort and varies by client.
    }
  }
}

function getPreviewThemeOverride() {
  const value = new URLSearchParams(window.location.search).get("preview_theme");
  return value === "dark" || value === "light" ? value : "";
}

function getTelegramBackgroundColor(theme = document.documentElement.dataset.theme) {
  if (telegram?.themeParams?.bg_color) {
    return telegram.themeParams.bg_color;
  }
  return theme === "dark" ? "#10161d" : "#f5efe5";
}

function isDarkColor(color) {
  const rgb = parseColor(color);
  if (!rgb) {
    return false;
  }

  const [red, green, blue] = rgb.map((channel) => {
    const value = channel / 255;
    return value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  const luminance = 0.2126 * red + 0.7152 * green + 0.0722 * blue;
  return luminance < 0.42;
}

function parseColor(color) {
  if (!color) {
    return null;
  }

  const value = String(color).trim();
  if (/^#[0-9a-f]{3}$/i.test(value)) {
    return value
      .slice(1)
      .split("")
      .map((part) => parseInt(part + part, 16));
  }

  if (/^#[0-9a-f]{6}$/i.test(value)) {
    return [1, 3, 5].map((start) => parseInt(value.slice(start, start + 2), 16));
  }

  const channels = value.match(/^rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
  if (!channels) {
    return null;
  }
  return channels.slice(1, 4).map(Number);
}

async function loadState() {
  renderLoading();
  try {
    const payload = await api("/api/state");
    state.user = payload.user;
    state.stories = payload.stories;
    state.session = payload.session;
    state.profile = payload.profile;
    state.feedback = null;
    state.completion = null;
    state.newAchievements = [];

    if (state.session) {
      renderSession();
    } else {
      renderLibrary();
    }
  } catch (error) {
    renderError(error.message);
  }
}

async function startStory(storyId) {
  renderLoading();
  pulse();
  try {
    const payload = await api(`/api/stories/${storyId}/start`, { method: "POST" });
    state.session = payload.session;
    state.profile = payload.profile;
    state.feedback = null;
    state.completion = null;
    state.newAchievements = [];
    renderSession();
  } catch (error) {
    renderError(error.message);
  }
}

async function startRandomStory() {
  renderLoading();
  pulse();
  try {
    const payload = await api("/api/stories/random/start", { method: "POST" });
    state.session = payload.session;
    state.profile = payload.profile;
    state.feedback = null;
    state.completion = null;
    state.newAchievements = [];
    renderSession();
  } catch (error) {
    renderError(error.message);
  }
}

async function submitAnswer(optionId) {
  renderLoading("Сохраняем выбор...");
  pulse();
  try {
    const payload = await api(`/api/answers/${optionId}`, { method: "POST" });
    state.feedback = payload.feedback;
    state.session = payload.session;
    state.profile = payload.profile;
    state.completion = payload.completion ?? null;
    state.newAchievements = payload.new_achievements ?? [];
    renderFeedback();
  } catch (error) {
    renderError(error.message);
  }
}

async function api(path, options = {}) {
  const headers = {
    "X-Telegram-Init-Data": telegram?.initData ?? "",
    ...options.headers,
  };

  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || "Не удалось выполнить запрос");
  }

  return response.json();
}

function renderLibrary() {
  state.screen = "library";
  setBackVisible(false);

  const fragment = document.createDocumentFragment();
  const head = el("section", "library-head");
  const titleBlock = el("div");
  titleBlock.append(
    el("p", "eyebrow", state.user?.is_development ? "Локальный preview" : "Telegram Mini App"),
    el("h2", "", "Выбор сюжета"),
    el("p", "", "Интерактивные истории о решениях, обстоятельствах и цене выбора.")
  );

  const actions = el("div", "library-actions");
  const randomButton = el("button", "primary-button", "Случайная история");
  randomButton.type = "button";
  randomButton.addEventListener("click", startRandomStory);
  actions.append(randomButton);
  head.append(titleBlock, actions);
  fragment.append(head);
  fragment.append(renderProfile());
  fragment.append(renderAchievements());

  if (!state.stories.length) {
    const empty = el("section", "state-panel");
    empty.append(el("p", "error-text", "Активные истории не найдены."));
    fragment.append(empty);
    appRoot.replaceChildren(fragment);
    return;
  }

  const grid = el("section", "story-grid");
  for (const story of state.stories) {
    grid.append(createStoryCard(story));
  }
  fragment.append(grid);
  appRoot.replaceChildren(fragment);
}

function renderProfile() {
  const profile = state.profile ?? {};
  const panel = el("section", "profile-panel");
  const head = el("div", "profile-head");
  const titleBlock = el("div");
  titleBlock.append(
    el("p", "eyebrow", "Профиль"),
    el("h2", "", formatUserName())
  );
  head.append(titleBlock);

  if (profile.continue_story) {
    const continueButton = el("button", "primary-button", "Продолжить");
    continueButton.type = "button";
    continueButton.addEventListener("click", () => {
      if (state.session) {
        renderSession();
      } else {
        loadState();
      }
    });
    head.append(continueButton);
  }

  const metrics = el("div", "profile-metrics");
  metrics.append(
    createProfileMetric(
      "Истории",
      `${profile.completed_stories ?? 0}/${profile.total_stories ?? state.stories.length}`,
      "пройдено"
    ),
    createProfileMetric(
      "Верные решения",
      `${profile.correct_percent ?? 0}%`,
      `${profile.correct_answers ?? 0} из ${profile.total_answers ?? 0}`
    )
  );

  const details = el("dl", "profile-details");
  details.append(
    createProfileDetail("Любимые темы", createTopicList(profile.favorite_topics ?? [])),
    createProfileDetail("Последняя история", profile.last_story?.title ?? "Пока нет прохождений")
  );

  panel.append(head, metrics, details);

  if (profile.continue_story) {
    panel.append(createContinueSummary(profile.continue_story));
  }

  return panel;
}

function renderAchievements() {
  const achievements = state.profile?.achievements ?? [];
  const summary = state.profile?.achievements_summary ?? {};
  const panel = el("section", "achievements-panel");
  const head = el("div", "achievements-head");
  const titleBlock = el("div");
  titleBlock.append(el("p", "eyebrow", "Достижения"), el("h2", "", "Коллекция"));
  head.append(titleBlock, el("strong", "", `${summary.unlocked ?? 0}/${summary.total ?? achievements.length}`));

  const list = el("div", "achievement-list");
  for (const achievement of achievements) {
    list.append(createAchievementItem(achievement));
  }

  panel.append(head, list);
  return panel;
}

function createAchievementItem(achievement) {
  const item = el("article", `achievement-item ${achievement.unlocked ? "is-unlocked" : "is-locked"}`);
  const marker = el("span", "achievement-marker", achievement.unlocked ? "✓" : `${achievement.progress_percent ?? 0}%`);
  marker.setAttribute("aria-hidden", "true");

  const copy = el("div", "achievement-copy");
  copy.append(el("strong", "", achievement.title), el("span", "", achievement.description));

  const meta = el("div", "achievement-meta");
  meta.append(el("span", "", achievement.unlocked ? "Получено" : `${achievement.progress}/${achievement.target}`));
  const progress = el("div", "achievement-progress");
  const bar = el("span");
  bar.style.width = `${achievement.progress_percent ?? 0}%`;
  progress.append(bar);
  meta.append(progress);

  item.append(marker, copy, meta);
  return item;
}

function createProfileMetric(label, value, hint) {
  const metric = el("div", "profile-metric");
  metric.append(el("span", "", label), el("strong", "", value), el("small", "", hint));
  return metric;
}

function createProfileDetail(term, content) {
  const item = el("div");
  const description = el("dd");
  if (content instanceof Node) {
    description.append(content);
  } else {
    description.textContent = content;
  }
  item.append(el("dt", "", term), description);
  return item;
}

function createTopicList(topics) {
  if (!topics.length) {
    return document.createTextNode("Появятся после первых ответов");
  }

  const list = el("div", "topic-list");
  for (const topic of topics) {
    list.append(el("span", "topic-chip", topic.name));
  }
  return list;
}

function createContinueSummary(story) {
  const summary = el("div", "continue-summary");
  const text = el("div");
  text.append(
    el("p", "eyebrow", `Этап ${story.current_step} из ${story.total_steps}`),
    el("strong", "", story.title)
  );

  const progress = el("div", "mini-progress");
  const bar = el("span");
  bar.style.width = `${story.progress_percent}%`;
  progress.append(bar);
  summary.append(text, progress);
  return summary;
}

function createStoryCard(story) {
  const card = storyCardTemplate.content.firstElementChild.cloneNode(true);
  card.querySelector("h2").textContent = story.title;
  card.querySelector("p:last-child").textContent = story.short_description;

  if (story.is_completed) {
    card.classList.add("is-completed");
    const status = el("span", "story-card__status", "Пройдено");
    status.title = "Этот сюжет уже завершён";
    card.querySelector(".story-card__kicker").after(status);
  }

  const button = card.querySelector("button");
  if (story.is_completed) {
    button.textContent = "Пройти заново";
    button.classList.remove("primary-button");
    button.classList.add("secondary-button");
  }
  button.addEventListener("click", () => startStory(story.id));
  return card;
}

function renderSession() {
  if (!state.session) {
    renderLibrary();
    return;
  }

  state.screen = "session";
  setBackVisible(true);

  const { story, step, progress } = state.session;
  const view = el("section", "story-view");
  const banner = el("div", "story-banner");
  banner.append(
    el("p", "eyebrow", `Этап ${progress.current} из ${progress.total}`),
    el("h2", "", story.title),
    el("p", "story-lead", story.short_description)
  );

  const body = el("div", "story-body");
  body.append(createProgress(progress));

  if (step.index === 1) {
    const intro = el("div", "intro-note");
    intro.append(el("p", "", story.intro_text));
    body.append(intro);
  }

  const text = el("div", "step-text");
  text.append(el("p", "", step.narrative_text));
  body.append(text);

  const question = el("section", "question-block");
  question.append(el("h3", "", step.question));
  const options = el("div", "options");
  step.options.forEach((option, index) => {
    const button = el("button", "option-button");
    button.type = "button";
    button.append(el("span", "option-index", String(index + 1)), el("span", "", option.text));
    button.addEventListener("click", () => submitAnswer(option.id));
    options.append(button);
  });
  question.append(options);
  body.append(question);

  const actions = el("div", "action-row");
  const libraryButton = el("button", "secondary-button", "К списку");
  libraryButton.type = "button";
  libraryButton.addEventListener("click", renderLibrary);
  actions.append(libraryButton);
  body.append(actions);

  view.append(banner, body);
  appRoot.replaceChildren(view);
}

function createProgress(progress) {
  const wrap = el("div", "progress-wrap");
  const line = el("div", "progress-line");
  line.append(el("span", "", "Прогресс"), el("span", "", `${progress.percent}%`));

  const track = el("div", "progress-track");
  const bar = el("div", "progress-bar");
  bar.style.width = `${progress.percent}%`;
  track.append(bar);
  wrap.append(line, track);
  return wrap;
}

function renderFeedback() {
  if (!state.feedback) {
    renderSession();
    return;
  }

  state.screen = "feedback";
  setBackVisible(true);

  const view = el("section", `feedback-view ${state.feedback.is_correct ? "is-correct" : "is-wrong"}`);
  const mark = el("div", "feedback-mark");
  mark.append(el("span", "feedback-pill", state.feedback.is_correct ? "Исторически верно" : "Разбор выбора"));
  mark.append(el("h2", "", state.feedback.verdict));

  const body = el("div", "feedback-body");
  const facts = el("dl", "fact-list");
  facts.append(
    createFact("Ваш вариант", state.feedback.selected_text),
    createFact("Что произошло", state.feedback.selected_outcome_text),
    createFact("Исторически верное решение", state.feedback.correct_text)
  );
  body.append(facts, el("p", "feedback-copy", state.feedback.explanation));
  const unlockedBlock = createUnlockedAchievementsBlock(state.newAchievements);
  if (unlockedBlock) {
    body.append(unlockedBlock);
  }

  const actions = el("div", "action-row");
  const continueButton = el("button", "primary-button", state.completion ? "Завершение" : "Продолжить");
  continueButton.type = "button";
  continueButton.addEventListener("click", () => {
    state.feedback = null;
    state.newAchievements = [];
    if (state.completion) {
      renderCompletion();
    } else {
      renderSession();
    }
  });
  actions.append(continueButton);
  body.append(actions);

  view.append(mark, body);
  appRoot.replaceChildren(view);
}

function createFact(term, description) {
  const item = el("div");
  item.append(el("dt", "", term), el("dd", "", description));
  return item;
}

function createUnlockedAchievementsBlock(achievements) {
  if (!achievements.length) {
    return null;
  }

  const block = el("section", "new-achievements");
  block.append(el("p", "eyebrow", achievements.length === 1 ? "Новое достижение" : "Новые достижения"));

  const list = el("div", "new-achievement-list");
  for (const achievement of achievements) {
    const item = el("div", "new-achievement-item");
    item.append(el("strong", "", achievement.title), el("span", "", achievement.description));
    list.append(item);
  }
  block.append(list);
  return block;
}

function renderCompletion() {
  if (!state.completion) {
    renderLibrary();
    return;
  }

  state.screen = "completion";
  setBackVisible(true);

  const view = el("section", "completion-view");
  const banner = el("div", "story-banner");
  banner.append(el("p", "eyebrow", "История завершена"), el("h2", "", state.completion.story_title));

  const body = el("div", "completion-body");
  body.append(el("p", "completion-copy", state.completion.outro_text));

  if (state.completion.editorial_sources?.length) {
    const sources = el("ul", "source-list");
    for (const source of state.completion.editorial_sources) {
      const item = el("li");
      const link = el("a", "", source.title);
      link.href = source.url;
      link.target = "_blank";
      link.rel = "noreferrer";
      item.append(link);
      sources.append(item);
    }
    body.append(sources);
  }

  const actions = el("div", "action-row");
  const shareButton = el("button", "primary-button share-button", "Поделиться");
  shareButton.type = "button";
  shareButton.title = "Поделиться результатом в Telegram";
  shareButton.addEventListener("click", shareCompletionResult);

  const listButton = el("button", "secondary-button", "Выбрать новую историю");
  listButton.type = "button";
  listButton.addEventListener("click", renderLibrary);
  actions.append(shareButton, listButton);
  body.append(actions);

  view.append(banner, body);
  appRoot.replaceChildren(view);
}

async function shareCompletionResult() {
  if (!state.completion) {
    return;
  }

  const text = formatCompletionShareText(state.completion);
  const telegramShareUrl = buildTelegramShareUrl(text);
  pulse();

  if (telegram?.openTelegramLink) {
    telegram.openTelegramLink(telegramShareUrl);
    return;
  }

  if (isMobileViewport() && navigator.share) {
    try {
      await navigator.share({ text });
      return;
    } catch (error) {
      if (error?.name === "AbortError") {
        return;
      }
    }
  }

  window.open(telegramShareUrl, "_blank", "noopener,noreferrer");
}

function formatCompletionShareText(completion) {
  const score = completion?.score ?? {};
  const correct = scoreNumber(score.correct_answers);
  const total = scoreNumber(score.total_steps ?? score.total_answers);

  if (correct !== null && total) {
    return `Я прошёл сюжет «${completion.story_title}»: ${correct}/${total} верных решений\n${SHARE_BOT_HANDLE}`;
  }

  return `Я прошёл сюжет «${completion.story_title}»\n${SHARE_BOT_HANDLE}`;
}

function scoreNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? number : null;
}

function buildTelegramShareUrl(text) {
  const params = new URLSearchParams({ url: SHARE_BOT_URL, text });
  return `https://t.me/share/url?${params.toString()}`;
}

function isMobileViewport() {
  return window.matchMedia?.("(hover: none) and (pointer: coarse)").matches ?? false;
}

function renderLoading(message = "Загрузка...") {
  state.screen = "loading";
  setBackVisible(false);
  const panel = el("section", "state-panel");
  panel.append(el("div", "loader"), el("p", "", message));
  appRoot.replaceChildren(panel);
}

function renderError(message) {
  state.screen = "error";
  setBackVisible(false);

  const panel = el("section", "state-panel");
  panel.append(el("p", "error-text", message));
  const retryButton = el("button", "primary-button", "Повторить");
  retryButton.type = "button";
  retryButton.addEventListener("click", loadState);
  panel.append(retryButton);
  appRoot.replaceChildren(panel);
}

function handleBack() {
  if (state.screen === "feedback") {
    state.feedback = null;
    state.newAchievements = [];
    if (state.completion) {
      renderCompletion();
    } else {
      renderSession();
    }
    return;
  }

  if (state.screen === "session" || state.screen === "completion") {
    renderLibrary();
    return;
  }

  telegram?.close?.();
}

function setBackVisible(isVisible) {
  backButton.hidden = !isVisible;

  if (!supportsTelegram("6.1") || !telegram?.BackButton) {
    return;
  }

  if (isVisible) {
    telegram.BackButton.show();
  } else {
    telegram.BackButton.hide();
  }
}

function pulse() {
  if (!supportsTelegram("6.1")) {
    return;
  }
  telegram?.HapticFeedback?.impactOccurred?.("light");
}

function supportsTelegram(version) {
  return Boolean(telegram?.isVersionAtLeast?.(version));
}

function formatUserName() {
  const name = [state.user?.first_name, state.user?.last_name].filter(Boolean).join(" ").trim();
  if (name) {
    return name;
  }
  if (state.user?.username) {
    return `@${state.user.username}`;
  }
  return "Ваш прогресс";
}

function el(tagName, className = "", textContent = "") {
  const node = document.createElement(tagName);
  if (className) {
    node.className = className;
  }
  if (textContent) {
    node.textContent = textContent;
  }
  return node;
}
