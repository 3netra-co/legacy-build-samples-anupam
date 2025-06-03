function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Campaign Generator')
    .addItem('Generate Campaign', 'triggerCampaign')
    .addItem('Add Content Details', 'addDetails')  // ✅ Add this line
    .addToUi();
}

function triggerCampaign() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('Prompt Sheet');
  const prompt = sheet.getRange('A1').getValue();

  const response = UrlFetchApp.fetch('http://3.85.165.188:5000/generate-campaign', {
    method: 'get',
    muteHttpExceptions: true
  });

  SpreadsheetApp.getUi().alert('Campaign generation triggered! Check "Output Tab" shortly.');
}
