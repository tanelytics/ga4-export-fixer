const { generateItemRevenueAssertionSql } = require('./itemRevenue.js');
const { generateDailyQualityAssertionSql } = require('./dailyQuality.js');

module.exports = {
    itemRevenue: generateItemRevenueAssertionSql,
    dailyQuality: generateDailyQualityAssertionSql,
};
