var startTime;

$(document).ready(function() {
  $("#refreshButton").click(function () { startRefresh0(); });
  // plus we may have extra processing to do:
  onReadyExtra();
  // before starting refresh
  startRefresh0();
});

function startRefresh0()
{	
	disableRefresh();
	beforeRefresh();
	startTime = new Date().getTime();
	startRefresh();
}

//"overridable" methods
function onReadyExtra() { }
function beforeRefresh() { }
function startRefresh() { }

function enableRefresh() {
  $("#refreshButton").css("visibility", "visible");
}

function disableRefresh() {
  $("#refreshButton").css("visibility", "hidden");
}

function handleError(status, msg) {
	enableRefresh();
	// print status code, message
	var statusStr = "Error";
	if (status) {
		statusStr += " ("+status+")";
	}
	statusStr += ": \""+msg+"\"";
	setTimeAndStatus(statusStr);
}

function setTimeAndStatus(status) {
  var taken = new Date().getTime() - startTime;
  $("#requestTime").text(taken/1000);
  if (status) {
	  $("#requestStatus").text(status);
  }
}
