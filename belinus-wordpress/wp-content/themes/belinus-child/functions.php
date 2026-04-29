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
	exit; // Exit if accessed directly.
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

	// Inter (Google Fonts) — explicit, async-friendly. Preconnect added in
	// belinus_resource_hints(). display=swap prevents FOIT.
	wp_enqueue_style(
		'belinus-google-fonts',
		'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap',
		array(),
		null
	);

	// Parent Divi style.
	wp_enqueue_style(
		'divi-parent-style',
		get_template_directory_uri() . '/style.css',
		array(),
		BELINUS_VERSION
	);

	// Child theme style (depends on parent + fonts).
	wp_enqueue_style(
		'belinus-child-style',
		get_stylesheet_uri(),
		array( 'divi-parent-style', 'belinus-google-fonts' ),
		BELINUS_VERSION
	);

	// Belinus JS bundle — in footer, no defer (needs to run after DOMContentLoaded
	// alongside GSAP/Motion.page which run synchronously).
	wp_enqueue_script(
		'belinus-main',
		BELINUS_THEME_URI . '/js/belinus.js',
		array(),
		BELINUS_VERSION,
		true
	);

	// Pass settings to JS via wp_localize_script.
	wp_localize_script(
		'belinus-main',
		'BELINUS',
		array(
			'restUrl'    => esc_url_raw( rest_url() ),
			'nonce'      => wp_create_nonce( 'wp_rest' ),
			'chatwootToken' => get_option( 'belinus_chatwoot_token', '' ),
			'calcomSlug' => get_option( 'belinus_calcom_slug', '' ),
			'reduce'     => false, // JS reads matchMedia at runtime.
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
 * Removes Divi's 58px top padding that pushes content below the nav,
 * and forces the hero section to the absolute top of the viewport.
 */
function belinus_critical_css(): void {
	echo "<style id='belinus-critical'>
#page-container{padding-top:0 !important;}
#main-content .container{padding-top:0 !important;}
.section-landing-hero.bl-hero{margin-top:0 !important;padding-top:0 !important;}
/* Force body visible — Motion.page sets body{visibility:hidden} which inline style cannot override */
body{visibility:visible !important;}
</style>\n";
}
add_action( 'wp_head', 'belinus_critical_css', 0 );
add_action( 'wp_footer', function(): void {
	echo "<script>
(function(){
  var revealed = false;
  function reveal() {
    if (revealed) return;
    revealed = true;
    // Override Motionpage's body{visibility:hidden} and
    // domcontentloaded visibility=inherit — the body must be visible
    // for GSAP pageLoad timelines to show hero content.
    document.body.style.visibility = 'visible';
  }
  // Attempt reveal at 300ms (after GSAP should have run pageLoad timelines).
  setTimeout(reveal, 300);
  // Also reveal on first user interaction.
  document.addEventListener('click', reveal, {once:true});
  document.addEventListener('scroll', reveal, {once:true, passive:true});
})();
</script>\n";
}, 9999 );

/**
 * Inline the Chatwoot + Cal.com loader scripts (deferred, only if configured).
 * No inline script for arbitrary code — only the canonical async snippets.
 */
function belinus_third_party_loaders(): void {
	$chatwoot_token = get_option( 'belinus_chatwoot_token', '' );
	if ( $chatwoot_token ) {
		?>
		<script>
		  (function(d,t) {
		    var BASE_URL = "https://chat.belinus.net";
		    var g = d.createElement(t), s = d.getElementsByTagName(t)[0];
		    g.src = BASE_URL + "/packs/sdk.js";
		    g.async = 1;
		    s.parentNode.insertBefore(g, s);
		    g.onload = function() {
		      window.chatwootSDK.run({
		        websiteToken: '<?php echo esc_js( $chatwoot_token ); ?>',
		        baseUrl: BASE_URL,
		        locale: 'en',
		        useBrowserLanguage: true
		      });
		    };
		  })(document, "script");
		</script>
		<?php
	}

	$calcom = get_option( 'belinus_calcom_slug', '' );
	if ( $calcom ) {
		?>
		<script>
			(function(C,A,L){let p=function(a,ar){a.q.push(ar);};let d=C.document;C.Cal=C.Cal||function(){let cal=C.Cal;let ar=arguments;if(!cal.loaded){cal.ns={};cal.q=cal.q||[];d.head.appendChild(d.createElement("script")).src=A;cal.loaded=true;if(ar[0]===L){const api=function(){p(api,arguments);};const namespace=ar[1];api.q=api.q||[];if(typeof namespace==="string"){cal.ns[namespace]=cal.ns[namespace]||api;p(cal.ns[namespace],ar);}else p(cal,ar);}else p(cal,ar);};}})(window,"https://app.cal.com/embed/embed.js","init");Cal("init",{origin:"https://cal.com"});
		</script>
		<?php
	}
}
	$calcom = get_option( 'belinus_calcom_slug', '' );
	if ( $calcom ) {
		?>
		<script>
			(function (C, A, L) {
				let p = function (a, ar) { a.q.push(ar); }; let d = C.document;
				C.Cal = C.Cal || function () {
					let cal = C.Cal; let ar = arguments;
					if (!cal.loaded) { cal.ns = {}; cal.q = cal.q || []; d.head.appendChild(d.createElement("script")).src = A; cal.loaded = true; }
					if (ar[0] === L) { const api = function () { p(api, arguments); }; const namespace = ar[1]; api.q = api.q || []; if (typeof namespace === "string") { cal.ns[namespace] = cal.ns[namespace] || api; p(cal.ns[namespace], ar); p(cal, ["initNamespace", namespace]); } else p(cal, ar); return; } p(cal, ar);
				};
			})(window, "https://app.cal.com/embed/embed.js", "init");
			Cal("init", { origin: "https://cal.com" });
		</script>
		<?php
	}
}
add_action( 'wp_footer', 'belinus_third_party_loaders', 5 );

/* =========================================================================
 * 2. THEME FEATURES
 * ========================================================================= */

function belinus_theme_setup(): void {
	add_theme_support( 'title-tag' );
	add_theme_support( 'post-thumbnails' );
	add_theme_support( 'html5', array( 'search-form', 'comment-form', 'comment-list', 'gallery', 'caption', 'script', 'style' ) );
	add_theme_support( 'custom-logo', array(
		'height'      => 48,
		'width'       => 180,
		'flex-width'  => true,
		'flex-height' => true,
	) );
	add_theme_support( 'responsive-embeds' );

	register_nav_menus( array(
		'primary' => __( 'Primary Navigation', 'belinus' ),
		'footer'  => __( 'Footer Navigation', 'belinus' ),
	) );

	load_child_theme_textdomain( 'belinus', BELINUS_THEME_DIR . '/languages' );
}
add_action( 'after_setup_theme', 'belinus_theme_setup' );

/* =========================================================================
 * 3. GDPR CONSENT TABLE
 * ========================================================================= */

/**
 * Create the wp_belinus_consent table on theme activation / upgrade.
 */
function belinus_create_consent_table(): void {
	global $wpdb;
	$table = $wpdb->prefix . 'belinus_consent';
	$charset = $wpdb->get_charset_collate();

	$sql = "CREATE TABLE {$table} (
		id BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
		submitted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		form_id BIGINT(20) UNSIGNED NOT NULL,
		email_hash CHAR(64) NOT NULL,
		ip_hash CHAR(64) NOT NULL,
		user_agent VARCHAR(512) NOT NULL DEFAULT '',
		consent_text TEXT NOT NULL,
		consent_version VARCHAR(32) NOT NULL DEFAULT '1.0',
		PRIMARY KEY (id),
		KEY idx_form (form_id),
		KEY idx_email (email_hash),
		KEY idx_submitted (submitted_at)
	) {$charset};";

	require_once ABSPATH . 'wp-admin/includes/upgrade.php';
	dbDelta( $sql );

	update_option( 'belinus_consent_table_version', '1.0' );
}

// Run once per theme activation OR if the version flag is missing.
function belinus_maybe_create_consent_table(): void {
	if ( get_option( 'belinus_consent_table_version' ) !== '1.0' ) {
		belinus_create_consent_table();
	}
}
add_action( 'after_switch_theme', 'belinus_create_consent_table' );
add_action( 'init', 'belinus_maybe_create_consent_table' );

/**
 * Insert one consent row.
 *
 * @param int    $form_id  CF7 form id.
 * @param string $email    Plain email (will be hashed).
 * @param string $ip       Plain IP (will be hashed).
 * @param string $ua       User agent string.
 * @param string $consent  The visible consent text shown to the user.
 */
function belinus_log_consent( int $form_id, string $email, string $ip, string $ua, string $consent ): void {
	global $wpdb;
	$wpdb->insert(
		$wpdb->prefix . 'belinus_consent',
		array(
			'submitted_at'    => current_time( 'mysql' ),
			'form_id'         => $form_id,
			'email_hash'      => hash( 'sha256', strtolower( trim( $email ) ) ),
			'ip_hash'         => hash( 'sha256', $ip . wp_salt( 'auth' ) ),
			'user_agent'      => substr( sanitize_text_field( $ua ), 0, 512 ),
			'consent_text'    => $consent,
			'consent_version' => '1.0',
		),
		array( '%s', '%d', '%s', '%s', '%s', '%s', '%s' )
	);
}

/* =========================================================================
 * 4. CF7 → HATCHET + DITTOFEED HOOK
 * ========================================================================= */

/**
 * Send Hatchet event + Dittofeed identify/track + log GDPR consent.
 * Fires inside CF7's wpcf7_before_send_mail (validated submission).
 */
function belinus_cf7_to_hatchet( $contact_form ): void {
	if ( ! $contact_form instanceof WPCF7_ContactForm ) {
		return;
	}

	$submission = WPCF7_Submission::get_instance();
	if ( ! $submission ) {
		return;
	}

	$data = $submission->get_posted_data();
	$meta = $submission->get_meta();

	// Bail if not the Belinus Lead form (configurable by slug).
	$lead_form_title = get_option( 'belinus_lead_form_title', 'Belinus Lead' );
	if ( $contact_form->title() !== $lead_form_title ) {
		return;
	}

	$email      = isset( $data['work-email'] ) ? sanitize_email( (string) $data['work-email'] ) : '';
	$full_name  = isset( $data['full-name'] )  ? sanitize_text_field( (string) $data['full-name'] ) : '';
	$company    = isset( $data['company'] )    ? sanitize_text_field( (string) $data['company'] ) : '';
	$role       = isset( $data['role'] )       ? sanitize_text_field( (string) $data['role'] ) : '';
	$site_type  = isset( $data['site-type'] )  ? sanitize_text_field( (string) $data['site-type'] ) : '';
	$peak_kw    = isset( $data['peak-demand'] )? sanitize_text_field( (string) $data['peak-demand'] ) : '';
	$message    = isset( $data['message'] )    ? sanitize_textarea_field( (string) $data['message'] ) : '';
	$gdpr       = ! empty( $data['gdpr-consent'] );

	if ( ! $email ) {
		return;
	}

	$ip_address = isset( $meta['remote_ip'] ) ? (string) $meta['remote_ip'] : '';
	$user_agent = isset( $meta['user_agent'] ) ? (string) $meta['user_agent'] : '';

	// Log GDPR consent (only if the box was actually ticked).
	if ( $gdpr ) {
		$consent_text = sprintf(
			/* translators: %s = privacy policy URL */
			__( 'I agree to be contacted by Belinus about my enquiry. See the privacy policy: %s', 'belinus' ),
			home_url( '/privacy' )
		);
		belinus_log_consent( $contact_form->id(), $email, $ip_address, $user_agent, $consent_text );
	}

	$payload_common = array(
		'email'        => $email,
		'name'         => $full_name,
		'company'      => $company,
		'role'         => $role,
		'site_type'    => $site_type,
		'peak_kw'      => $peak_kw,
		'message'      => $message,
		'gdpr_consent' => $gdpr,
		'form_id'      => $contact_form->id(),
		'form_title'   => $contact_form->title(),
		'submitted_at' => current_time( 'c' ),
		'source_url'   => isset( $meta['url'] ) ? esc_url_raw( (string) $meta['url'] ) : home_url( '/' ),
	);

	belinus_send_to_hatchet( $payload_common );
	belinus_send_to_dittofeed( $payload_common );
}
add_action( 'wpcf7_before_send_mail', 'belinus_cf7_to_hatchet', 20, 1 );

/**
 * Non-blocking POST to Hatchet event endpoint.
 */
function belinus_send_to_hatchet( array $payload ): void {
	$url = get_option( 'belinus_hatchet_url', '' );
	$key = get_option( 'belinus_hatchet_key', '' );

	if ( ! $url || ! $key ) {
		return;
	}

	$body = wp_json_encode( array(
		'event' => 'wordpress:lead_captured',
		'data'  => $payload,
	) );

	wp_remote_post( $url, array(
		'method'   => 'POST',
		'timeout'  => 0.01,            // Non-blocking — don't wait.
		'blocking' => false,
		'headers'  => array(
			'Content-Type'  => 'application/json',
			'Authorization' => 'Bearer ' . $key,
		),
		'body'     => $body,
	) );
}

/**
 * Non-blocking identify + track to Dittofeed.
 */
function belinus_send_to_dittofeed( array $payload ): void {
	$base_url = get_option( 'belinus_dittofeed_url', '' );
	$api_key  = get_option( 'belinus_dittofeed_key', '' );

	if ( ! $base_url || ! $api_key ) {
		return;
	}

	$base_url = untrailingslashit( $base_url );
	$user_id  = hash( 'sha256', strtolower( trim( $payload['email'] ) ) );

	$identify_body = wp_json_encode( array(
		'userId' => $user_id,
		'traits' => array(
			'email'     => $payload['email'],
			'name'      => $payload['name'],
			'company'   => $payload['company'],
			'role'      => $payload['role'],
			'site_type' => $payload['site_type'],
		),
	) );

	$track_body = wp_json_encode( array(
		'userId'     => $user_id,
		'event'      => 'lead_captured',
		'properties' => array(
			'form_id'    => $payload['form_id'],
			'form_title' => $payload['form_title'],
			'peak_kw'    => $payload['peak_kw'],
			'source_url' => $payload['source_url'],
		),
	) );

	$args = array(
		'timeout'  => 0.01,
		'blocking' => false,
		'headers'  => array(
			'Content-Type'  => 'application/json',
			'Authorization' => 'Bearer ' . $api_key,
		),
	);

	wp_remote_post( $base_url . '/api/public/apps/identify', array_merge( $args, array( 'body' => $identify_body ) ) );
	wp_remote_post( $base_url . '/api/public/apps/track',    array_merge( $args, array( 'body' => $track_body    ) ) );
}

/* =========================================================================
 * 5. CF7 — REJECT FREE-MAIL DOMAINS ON work-email FIELD
 * ========================================================================= */

/**
 * Validation hook: returns invalid for common free-mail providers on
 * any field whose name matches `work-email`.
 */
function belinus_reject_freemail_email( $result, $tag ) {
	$tag_name = is_object( $tag ) ? $tag->name : ( $tag['name'] ?? '' );
	if ( 'work-email' !== $tag_name ) {
		return $result;
	}

	$value = isset( $_POST[ $tag_name ] ) ? sanitize_email( wp_unslash( (string) $_POST[ $tag_name ] ) ) : '';
	if ( ! $value ) {
		return $result;
	}

	$domain = strtolower( substr( strrchr( $value, '@' ), 1 ) );
	$blocked = array(
		'gmail.com', 'googlemail.com',
		'hotmail.com', 'hotmail.co.uk', 'hotmail.fr', 'hotmail.be', 'hotmail.nl',
		'outlook.com', 'outlook.fr', 'outlook.be', 'outlook.nl',
		'live.com', 'live.fr', 'live.be', 'live.nl',
		'msn.com',
		'yahoo.com', 'yahoo.fr', 'yahoo.co.uk', 'yahoo.es',
		'proton.me', 'protonmail.com', 'pm.me',
		'gmx.com', 'gmx.net', 'gmx.de',
		'mail.com',
		'icloud.com', 'me.com', 'mac.com',
		'aol.com',
		'zoho.com',
		'yandex.com', 'yandex.ru',
	);

	if ( in_array( $domain, $blocked, true ) ) {
		$result->invalidate( $tag, __( 'Please use a work email — we don\'t accept personal mail providers for B2B enquiries.', 'belinus' ) );
	}

	return $result;
}
add_filter( 'wpcf7_validate_email',  'belinus_reject_freemail_email', 10, 2 );
add_filter( 'wpcf7_validate_email*', 'belinus_reject_freemail_email', 10, 2 );

/* =========================================================================
 * 6. SETTINGS PAGE — Belinus integrations
 * ========================================================================= */

function belinus_register_settings_page(): void {
	add_options_page(
		__( 'Belinus Integrations', 'belinus' ),
		__( 'Belinus', 'belinus' ),
		'manage_options',
		'belinus-settings',
		'belinus_render_settings_page'
	);
}
add_action( 'admin_menu', 'belinus_register_settings_page' );

function belinus_register_settings(): void {
	$fields = array(
		'belinus_chatwoot_token' => 'Chatwoot Website Token',
		'belinus_calcom_slug'   => 'Cal.com slug (e.g. belinus/discovery-30)',
		'belinus_hatchet_url'   => 'Hatchet event URL',
		'belinus_hatchet_key'   => 'Hatchet API key',
		'belinus_dittofeed_url' => 'Dittofeed base URL',
		'belinus_dittofeed_key' => 'Dittofeed API key',
		'belinus_lead_form_title' => 'CF7 lead form title (default: Belinus Lead)',
	);

	foreach ( $fields as $key => $label ) {
		register_setting(
			'belinus_settings_group',
			$key,
			array(
				'type'              => 'string',
				'sanitize_callback' => str_ends_with( $key, '_url' ) ? 'esc_url_raw' : 'sanitize_text_field',
				'default'           => '',
			)
		);
	}
}
add_action( 'admin_init', 'belinus_register_settings' );

function belinus_render_settings_page(): void {
	if ( ! current_user_can( 'manage_options' ) ) {
		return;
	}
	$fields = array(
		'belinus_chatwoot_token'  => 'Chatwoot Website Token',
		'belinus_calcom_slug'     => 'Cal.com slug',
		'belinus_hatchet_url'     => 'Hatchet event URL',
		'belinus_hatchet_key'     => 'Hatchet API key',
		'belinus_dittofeed_url'   => 'Dittofeed base URL',
		'belinus_dittofeed_key'   => 'Dittofeed API key',
		'belinus_lead_form_title' => 'CF7 lead form title',
	);
	?>
	<div class="wrap">
		<h1><?php esc_html_e( 'Belinus Integrations', 'belinus' ); ?></h1>
		<p><?php esc_html_e( 'Configure third-party integrations for the Belinus child theme. Empty fields disable that integration silently.', 'belinus' ); ?></p>
		<form method="post" action="options.php">
			<?php settings_fields( 'belinus_settings_group' ); ?>
			<table class="form-table" role="presentation">
				<tbody>
					<?php foreach ( $fields as $key => $label ) : ?>
						<tr>
							<th scope="row"><label for="<?php echo esc_attr( $key ); ?>"><?php echo esc_html( $label ); ?></label></th>
							<td>
								<input
									type="<?php echo str_ends_with( $key, '_key' ) ? 'password' : 'text'; ?>"
									id="<?php echo esc_attr( $key ); ?>"
									name="<?php echo esc_attr( $key ); ?>"
									value="<?php echo esc_attr( get_option( $key, '' ) ); ?>"
									class="regular-text"
									autocomplete="off"
								/>
							</td>
						</tr>
					<?php endforeach; ?>
				</tbody>
			</table>
			<?php submit_button(); ?>
		</form>
	</div>
	<?php
}

/* =========================================================================
 * 7. DEVELOPER NOTES (kept in code so they don't drift)
 * =========================================================================
 *
 * - All animation reveal classes (.bl-reveal-up, .bl-stagger) are toggled
 *   by /js/belinus.js on IntersectionObserver entry. Motion.page can also
 *   target them directly — they coexist.
 *
 * - The ROI calculator markup lives in a Divi Code module on the page.
 *   The HTML must use class `bl-roi-calculator` for the JS to bind.
 *
 * - Crisp + Cal.com loaders are inlined here only so they pick up the
 *   admin-configured IDs; everything else lives in belinus.js.
 *
 * - Never hard-code API keys or webhook URLs in this file. Always read
 *   from get_option() and configure via Settings → Belinus.
 *
 * ========================================================================= */
