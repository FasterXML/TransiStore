
function sendRequest(url, okFunction)
{
    $("#requestUrl").text(url);
    $.ajax({
        type : "GET",
        url : url,
        contentType : "application/json",
//        data : json,
/* 26-Apr-2012, tatu: Let's force response content type as "text" (wrt jQuery) to avoid
 *   jQuery's seemingly buggy parsing and rather rely on explicit fast native parsing by Firefox
 */
        dataType : "text",
        success : function (results, status) {
          setTimeAndStatus(status);
          var json = parseJSON(results); 
          if (!json) {
              setFailure(status, "Invalid JSON, could not parse: "+results);
              return;
      	  }
      	  okFunction(json);
        },
        error : function(xhr, err, msg) {
       	  var status = xhr.status;
          if (!status) {
              status = err;
          }
          handleError(status, msg); //xhr.responseText);
        }
    });
}

/* Ideally JSON parsing would work well natively, but looks
 * like there are caveats, so let's separate details in a
 * function
 */
function parseJSON(msg)
{
  // Alas: Jquery-json sucks, need to use plain eval
  try {
  // return  $.evalJSON(msg);
    // If we have new-fangled JSON package, this rocks:
	if (JSON) {
    	return JSON.parse(msg);
	}
	// otherwise this ought to work:
    return eval("(" + msg + ")");
  } catch (x) {
    //alert("Invalid JSON: "+x);
    return null;
  }
}

function verifyJSON(msg)
{
    setTime();
    var json = parseJSON(msg); 
    if (!json) {
        setFailure(-1, "Invalid JSON, could not parse: "+msg);
	}
	return json;
}
