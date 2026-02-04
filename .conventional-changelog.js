module.exports = {
  preset: "conventionalcommits",
  presetConfig: {
    types: [
      {
        type: "feat",
        section: "✨ Features ✨",
        hidden: false,
      },
      {
        type: "fix",
        section: "🐛 Bug Fixes 🐛",
        hidden: false,
      },
      {
        type: "perf",
        section: "⚡️ Performance Improvements ⚡️",
        hidden: false,
      },
      {
        type: "revert",
        section: "⏮️️ Reverts ⏮️️",
        hidden: false,
      },
      {
        type: "build",
        section: "⚙️ Build ⚙️",
        hidden: false,
      },
      {
        type: "refactor",
        section: "♻️ Refactors ♻️",
        hidden: false,
      },
    ],
  },
};
