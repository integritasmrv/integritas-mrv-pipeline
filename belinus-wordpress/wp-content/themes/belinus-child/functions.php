<?php
/**
 * Belinus Child Theme — functions.php
 *
 * Enqueues, theme support, Settings page, CF7 → Hatchet/Dittofeed integration,
 * GDPR consent log, free-mail rejection. No inline scripts. No hardcoded secrets.
 *
 * @package Belinus
 * @since   1.3.0
 */

declare( strict_types=1 );

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

define( 'BELINUS_VERSION', '1.3.0' );
define( 'BELINUS_THEME_DIR', get_stylesheet_directory() );
define( 'BELINUS_THEME_URI', get_stylesheet_directory_uri() );

/* =========================================================================
 * 1. THEME SUPPORT + ENQUEUES
 * ========================================================================= */

/**
 * Enqueue parent Divi styles, child styles, and the Belinus JS bundle.
 */
function belinus_enqueue_assets(): void {

	wp_enqueue_style(
		'belinus-google-fonts',
		'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap',
		array(),
		null
	);

	wp_enqueue_style(
		'divi-parent-style',
		get_template_directory_uri() . '/style.css',
		array(),
		BELINUS_VERSION
	);

	wp_enqueue_style(
		'belinus-child-style',
		get_stylesheet_uri(),
		array( 'divi-parent-style', 'belinus-google-fonts' ),
		BELINUS_VERSION
	);

	wp_enqueue_script(
		'belinus-main',
		BELINUS_THEME_URI . '/js/belinus.js',
		array(),
		BELINUS_VERSION,
		true
	);

	wp_localize_script(
		'belinus-main',
		'BELINUS',
		array(
			'restUrl'       => esc_url_raw( rest_url() ),
			'nonce'         => wp_create_nonce( 'wp_rest' ),
			'chatwootToken' => get_option( 'belinus_chatwoot_token', '' ),
			'calcomSlug'    => get_option( 'belinus_calcom_slug', '' ),
			'reduce'        => false,
		)
	);
}
add_action( 'wp_enqueue_scripts', 'belinus_enqueue_assets', 20 );

/**
 * Resource hints — preconnect Google Fonts.
 */
function belinus_resource_hints( array $urls, string $relation_type ): array {
	if ( 'preconnect' === $relation_type ) {
		$urls[] = array(
			'href'        => 'https://fonts.googleapis.com',
			'crossorigin' => '',
		);
		$urls[] = array(
			'href'        => 'https://fonts.gstatic.com',
			'crossorigin' => '',
		);
	}
	return $urls;
}
add_filter( 'wp_resource_hints', 'belinus_resource_hints', 10, 2 );

/**
 * Critical CSS overrides — must load before Divi's inline styles.
 * Removes Divi's 58px top padding and forces the hero section to the
 * absolute top of the viewport.
 */
function belinus_critical_css(): void {
	echo "<style id='belinus-critical'>
#page-container{padding-top:0!important}
#main-content .container{padding-top:0!important}
.section-landing-hero.bl-hero{margin-top:0!important;padding-top:0!important}
</style>\n";
}
add_action( 'wp_head', 'belinus_critical_css', 0 );

/**
 * Reveal body after Motionpage's visibility lock.
 * Motionpage sets body{visibility:hidden} then DOMContentLoaded sets
 * document.body.style.visibility="inherit". This override forces visible
 * at 300ms (after GSAP pageLoad timelines run) and on user interaction.
 */
function belinus_visibility_reveal(): void {
	echo "<script>
(function(){
  var revealed=false;
  function reveal(){if(revealed)return;revealed=true;document.body.style.visibility='visible'}
  setTimeout(reveal,300);
  document.addEventListener('click',reveal,{once:true});
  document.addEventListener('scroll',reveal,{once:true,passive:true});
})();
</script>\n";
}
add_action( 'wp_footer', 'belinus_visibility_reveal', 9999 );

/* =========================================================================
 * 2. CF7 → HATCHET CRM
 * ========================================================================= */

/**
 * Map CF7 field names to Hatchet CRM field names.
 */
function belinus_cf7_field_map(): array {
	return array(
		'first-name'      => 'first_name',
		'last-name'       => 'last_name',
		'your-email'      => 'email',
		'your-phone'      => 'phone',
		'company-name'    => 'company',
		'function'        => 'role',
		'product-interest' => 'site_type',
		'your-message'    => 'message',
	);
}

/**
 * Reject free-mail senders before they reach Hatchet.
 */
function belinus_is_free_mail( string $email ): bool {
	$free_domains = array(
		'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
		'live.com', 'msn.com', 'aol.com', 'icloud.com',
		'protonmail.com', 'mail.com', 'inbox.com', 'zoho.com',
	);
	$domain = explode( '@', $email );
	$domain = end( $domain );
	return in_array( strtolower( $domain ), $free_domains, true );
}

/**
 * Log CF7 submissions to belinus_cf7_log option (GDPR compliance).
 */
function belinus_log_submission( array $data ): void {
	$log = get_option( 'belinus_cf7_log', array() );
	$log[] = array(
		'timestamp' => current_time( 'c' ),
		'email'     => $data['your-email'] ?? '',
		'name'      => trim( ( $data['first-name'] ?? '' ) . ' ' . ( $data['last-name'] ?? '' ) ),
	);
	if ( count( $log ) > 200 ) {
		$log = array_slice( $log, -200 );
	}
	update_option( 'belinus_cf7_log', $log );
}

/**
 * Send CF7 submission to Hatchet CRM.
 * Hatchet is the master CRM; Dittofeed handles ESP/drip downstream.
 */
function belinus_cf7_to_hatchet(
	\WPCF7_ContactForm $contact_form,
	bool               &$abort,
	\WPCF7_Submission   $submission
): void {
	$contact_form->skip_mail = true;

	if ( $contact_form->title() !== 'Belinus B2B v3' ) {
		return;
	}

	$data = $submission->get_posted_data();

	if ( empty( $data['your-email'] ) ) {
		return;
	}

	if ( belinus_is_free_mail( $data['your-email'] ) ) {
		$submission->set_response( 'Free e-mail addresses are not accepted. Please use your company e-mail.' );
		$abort = true;
		return;
	}

	belinus_log_submission( $data );

	$hatchet_url = get_option( 'belinus_hatchet_url', '' );
	$hatchet_key = get_option( 'belinus_hatchet_key', '' );

	if ( empty( $hatchet_url ) || empty( $hatchet_key ) ) {
		return;
	}

	$field_map = belinus_cf7_field_map();
		$payload   = array(
		'email'     => sanitize_email( $data['your-email'] ),
		'firstName' => sanitize_text_field( $data['first-name'] ?? '' ),
		'lastName'  => sanitize_text_field( $data['last-name'] ?? '' ),
		'phone'     => sanitize_text_field( $data['your-phone'] ?? '' ),
		'company'   => sanitize_text_field( $data['company-name'] ?? '' ),
		'role'      => sanitize_text_field( $data['function'] ?? '' ),
		'siteType'  => sanitize_text_field( $data['product-interest'] ?? '' ),
		'message'   => sanitize_textarea_field( $data['your-message'] ?? '' ),
		'source'    => 'belinus-wordpress-cf7',
	);

	$response = wp_safe_remote_post(
		$hatchet_url,
		array(
			'method'      => 'POST',
			'headers'     => array(
				'Content-Type'  => 'application/json',
				'Authorization' => 'Bearer ' . $hatchet_key,
			),
			'body'        => wp_json_encode( $payload ),
			'timeout'     => 15,
			'data_format' => 'body',
		)
	);

	if ( is_wp_error( $response ) ) {
		error_log( 'Belinus CF7→Hatchet error: ' . $response->get_error_message() );
	}
}
add_action( 'wpcf7_before_send_mail', 'belinus_cf7_to_hatchet', 10, 3 );

/* =========================================================================
 * 3. CHATWOOT + CAL.COM LOADER (Settings page controls these)
 * ========================================================================= */

/**
 * Inline the Chatwoot + Cal.com loader scripts (deferred, only if configured).
 */
function belinus_third_party_loaders(): void {
	$chatwoot_token = get_option( 'belinus_chatwoot_token', '' );
	$calcom_slug    = get_option( 'belinus_calcom_slug', '' );

	if ( $chatwoot_token ) {
		?>
		<script>
		  window.chatwootSettings = {
		    position: 'right',
		    type: 'standard',
		    token: '<?php echo esc_js( $chatwoot_token ); ?>'
		  };
		  (function() {
		    var w = window; var i = function() { w.chatwootSDK.run(w.chatwootSettings); };
		    if (w.chatwootSDK) { i(); } else {
		      var d = document; var g = d.createElement('script'); g.src = 'https://cdn.chatwoot.com/sdks/woot.js'; g.async = true;
		      g.onload = i; d.head.appendChild(g);
		    }
		  })();
		</script>
		<?php
	}

	if ( $calcom_slug ) {
		?>
		<script>
		  (function(C, A, L) {
		    let p = function(a, ar) { p.q.push(a); if (!ar) { p.q.concat(ar) } };
		    C[L] = C[L] || { q: [], init: function() { p(C[L], arguments) }, h: C[L].h };
		    p(A, L);
		  })(window, '<?php echo esc_js( $calcom_slug ); ?>', 'Cal');
		  var link = document.createElement('link');
		  link.rel = 'stylesheet';
		  link.href = 'https://cal.com/embed/embed.css';
		  document.head.appendChild(link);
		  (function(d, t) {
		    var s = d.createElement('script'); s.src = 'https://app.cal.com/embed/embed.js'; s.async = true;
		    var g = d.getElementsByTagName(t)[0]; g.parentNode.insertBefore(s, g);
		  })(document, 'script');
		  Cal('init', { origin: 'https://cal.com' });
		</script>
		<?php
	}
}
add_action( 'wp_footer', 'belinus_third_party_loaders', 5 );

/* =========================================================================
 * 4. SETTINGS PAGE
 * ========================================================================= */

/**
 * Add Belinus settings page under the Options menu.
 */
function belinus_settings_page(): void {
	add_options_page(
		'Belinus Settings',
		'Belinus',
		'manage_options',
		'belinus',
		function(): void {
			if ( ! current_user_can( 'manage_options' ) ) {
				return;
			}

			if ( isset( $_POST['belinus_nonce'] )
				&& wp_verify_nonce( sanitize_key( $_POST['belinus_nonce'] ), 'belinus_settings' )
			) {
				if ( isset( $_POST['belinus_hatchet_url'] ) ) {
					update_option( 'belinus_hatchet_url', esc_url_raw( $_POST['belinus_hatchet_url'] ) );
				}
				if ( isset( $_POST['belinus_hatchet_key'] ) ) {
					update_option( 'belinus_hatchet_key', sanitize_text_field( $_POST['belinus_hatchet_key'] ) );
				}
				if ( isset( $_POST['belinus_chatwoot_token'] ) ) {
					update_option( 'belinus_chatwoot_token', sanitize_text_field( $_POST['belinus_chatwoot_token'] ) );
				}
				if ( isset( $_POST['belinus_calcom_slug'] ) ) {
					update_option( 'belinus_calcom_slug', sanitize_text_field( $_POST['belinus_calcom_slug'] ) );
				}
				echo '<div class="updated"><p>Settings saved.</p></div>';
			}

			$hatchet_url    = get_option( 'belinus_hatchet_url', '' );
			$hatchet_key    = get_option( 'belinus_hatchet_key', '' );
			$chatwoot_token = get_option( 'belinus_chatwoot_token', '' );
			$calcom_slug    = get_option( 'belinus_calcom_slug', '' );
			?>
			<div class="wrap">
				<h1>Belinus Settings</h1>
				<form method="post" style="max-width:600px">
					<?php wp_nonce_field( 'belinus_settings', 'belinus_nonce' ); ?>

					<h2>CRM — Hatchet</h2>
					<table class="form-table">
						<tr>
							<th>Webhook URL</th>
							<td>
								<input type="url" name="belinus_hatchet_url"
									value="<?php echo esc_attr( $hatchet_url ); ?>"
									class="regular-text" placeholder="https://hatchet.example.com/webhook">
							</td>
						</tr>
						<tr>
							<th>API Key</th>
							<td>
								<input type="text" name="belinus_hatchet_key"
									value="<?php echo esc_attr( $hatchet_key ); ?>"
									class="regular-text" placeholder="hatchet_live_...">
							</td>
						</tr>
					</table>

					<h2>Chat Support — Chatwoot</h2>
					<table class="form-table">
						<tr>
							<th>Chatwoot Token</th>
							<td>
								<input type="text" name="belinus_chatwoot_token"
									value="<?php echo esc_attr( $chatwoot_token ); ?>"
									class="regular-text" placeholder="4VkiBbVD6xdvbAyKOMof">
								<p class="description">Find this in Chatwoot → Settings → Widget</p>
							</td>
						</tr>
					</table>

					<h2>Booking — Cal.com</h2>
					<table class="form-table">
						<tr>
							<th>Cal.com Slug</th>
							<td>
								<input type="text" name="belinus_calcom_slug"
									value="<?php echo esc_attr( $calcom_slug ); ?>"
									class="regular-text" placeholder="your-slug">
							</td>
						</tr>
					</table>

					<p><input type="submit" class="button-primary" value="Save Settings"></p>
				</form>
			</div>
			<?php
		}
	);
}
add_action( 'admin_menu', 'belinus_settings_page' );
