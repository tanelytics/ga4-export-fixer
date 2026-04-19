const { generateDailyQualityAssertionSql, _generateDailyQualityAssertionSql } = require('./dailyQuality.js');

module.exports = {
    dailyQuality: generateDailyQualityAssertionSql,
    _internal: {
        dailyQuality: { generate: _generateDailyQualityAssertionSql, defaultName: 'daily_quality' },
    },
};
