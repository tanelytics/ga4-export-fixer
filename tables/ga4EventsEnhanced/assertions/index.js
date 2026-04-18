const { generateItemRevenueAssertionSql, _generateItemRevenueAssertionSql } = require('./itemRevenue.js');
const { generateDailyQualityAssertionSql, _generateDailyQualityAssertionSql } = require('./dailyQuality.js');

module.exports = {
    itemRevenue: generateItemRevenueAssertionSql,
    dailyQuality: generateDailyQualityAssertionSql,
    _internal: {
        dailyQuality: { generate: _generateDailyQualityAssertionSql, defaultName: 'daily_quality' },
        itemRevenue: { generate: _generateItemRevenueAssertionSql, defaultName: 'item_revenue', enabledByDefault: false },
    },
};
