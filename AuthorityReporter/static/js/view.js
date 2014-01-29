$(document).ready(function() {
	$("#search-main").submit(function(event){
		var q = $("#q-main").val();
		if (q.length > 0) {
		    window.location = '/#/search/'+encodeURIComponent(q)+'/';
		    return false;
		}
	    });

	$(".close").click(function() {
		window.close();
	    });
    });