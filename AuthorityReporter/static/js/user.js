var signup = false;

handleLogin = function() {
		var username = jQuery('#user-user').val().trim();
		var pass1 = jQuery('#user-pass').val();
		var pass2 = jQuery('#user-pass-confirm').val();
		if (username == '') {
		    jQuery('#user-user').text("Enter a username");
		    return false;
		}
		if (signup) {
		    if (pass1 != pass2) {
			jQuery('#user-login-feedback').text("Passwords don't match");
			return false;
		    }
		    jQuery.ajax({type:"POST", url:"/user/create", data:{user:username, pass:pass1}, success:function(response) {
				var response = jQuery.parseJSON(response);
				if (response['status'] !== 200) {
				    jQuery('#user').html(response['response']);
				} else {
				    jQuery('#user').html(response['response']);
				}
			    }});
		} else {
		    jQuery.ajax({type:"POST", url:"/user/login", data:{user:username, pass:pass1}, success:function(response) {
				var response = jQuery.parseJSON(response);
				jQuery('#user').html(response['response']);
			    }});
		}
		return false;
            };

jQuery(document).ready(function() {

	jQuery('#user').on('click', '#user-is-signup', function() {
		self = jQuery(this);
		signup = self.prop('checked');
		if (!signup) {
		    jQuery('#user-pass-confirm').css('visibility', 'hidden').attr('disabled', 'disabled');
		} else {
		    jQuery('#user-pass-confirm').css('visibility', 'visible').removeAttr('disabled');
		}
	    });

	jQuery('#user').on('click', '#user-logout', function() {
		jQuery.ajax({type:"POST", url:"/user/logout", success:function(response) {
				var response = jQuery.parseJSON(response);
				jQuery('#user').html(response['response']);
			}});
		return false;
	    });

	jQuery('#user').on('click', '#user-favorites', function() {
		currentUrl = '/user/faves';
		page = 1;
		pagination();
		return false;
	    });


	jQuery('#user').on('submit', '#user-login', handleLogin );

	jQuery('#results').on('click', '.favorite-star.glyphicon-star-empty.star-enabled', function () {
		var dom = jQuery(this);
		
	    });

	jQuery('#results').on('click', '.favorite-star.glyphicon-star.star-enabled', function () {
		var dom = jQuery(this);
		if (dom.hasClass('faved')) {
			jQuery.ajax({type:"POST", url:"/user/unfave", data:{'id':dom.data('doc')}, success:function(response) {
			    dom.removeClass('glyphicon-star').addClass('glyphicon-star-empty');
			}});
		    } else {
			jQuery.ajax({type:"POST", url:"/user/fave", data:{'id':dom.data('doc')}, success:function(response) {
			    dom.removeClass('glyphicon-star-empty').addClass('glyphicon-star').addClass('faved');
			}});
		    }
	    });

	jQuery('#results').on('mouseover', '.favorite-star.glyphicon-star-empty.star-enabled', function() {
		jQuery(this).removeClass('glyphicon-star-empty').addClass('glyphicon-star');
	    });

	jQuery('#results').on('mouseleave', '.favorite-star.glyphicon-star.star-enabled', function() {
		var dom = jQuery(this);
		if (! dom.hasClass('faved') ) {
			dom.removeClass('glyphicon-star').addClass('glyphicon-star-empty');
		    }
	    });

    });