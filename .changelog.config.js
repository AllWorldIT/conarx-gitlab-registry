// Type configuration for conventional-changelog-cli
//
// NOTE: The type-to-section mapping is also defined in .releaserc.yml for
// semantic-release. If you update the types here, update them there too.

const typeOrder = ['feat', 'fix', 'perf', 'revert', 'build', 'refactor']

const typeMap = {
  feat: '✨ Features ✨',
  fix: '🐛 Bug Fixes 🐛',
  perf: '⚡️ Performance Improvements ⚡️',
  revert: '⏮️️ Reverts ⏮️️',
  build: '⚙️ Build ⚙️',
  refactor: '♻️ Refactors ♻️'
}

// Reverse lookup: section title -> original type
const titleToType = Object.fromEntries(
  Object.entries(typeMap).map(([k, v]) => [v, k])
)

module.exports = {
  writerOpts: {
    transform: (commit, context) => {
      if (!commit.type || !typeMap[commit.type]) {
        return null
      }

      commit.type = typeMap[commit.type]

      if (typeof commit.hash === 'string') {
        commit.shortHash = commit.hash.substring(0, 7)
      }

      return commit
    },
    groupBy: 'type',
    commitGroupsSort: (a, b) => {
      // Sort by the original type order, not alphabetically
      const aType = titleToType[a.title] || a.title
      const bType = titleToType[b.title] || b.title
      return typeOrder.indexOf(aType) - typeOrder.indexOf(bType)
    },
    commitsSort: ['scope', 'subject']
  }
}
