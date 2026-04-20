<?php
/**
 * Plugin Name: Belinus CF7 to API
 * Description: Captures CF7 submissions and forwards to PowerIQ CRM API
 */
add_action('wpcf7_before_mail', 'belinus_before_mail', 10, 1);
add_action('wpcf7_mail_sent', 'belinus_mail_sent', 10, 1);
add_action('wpcf7_mail_failed', 'belinus_mail_failed', 10, 1);

function belinus_capture_and_send() {
    $s = WPCF7_Submission::get_instance();
    if (!$s) { return false; }
    $d = $s->get_posted_data();
    if (empty($d)) { return false; }
    $p = array(
        'first-name' => isset($d['first-name']) ? sanitize_text_field($d['first-name']) : '',
        'last-name' => isset($d['last-name']) ? sanitize_text_field($d['last-name']) : '',
        'your-email' => isset($d['your-email']) ? sanitize_email($d['your-email']) : '',
        'phone' => isset($d['phone']) ? sanitize_text_field($d['phone']) : '',
        'company' => isset($d['company']) ? sanitize_text_field($d['company']) : '',
        'company-website' => isset($d['company-website']) ? esc_url_raw($d['company-website']) : '',
        'function' => isset($d['function']) ? sanitize_text_field($d['function']) : '',
        'product-interest' => isset($d['product-interest']) ? sanitize_text_field($d['product-interest']) : '',
        'message' => isset($d['message']) ? sanitize_textarea_field($d['message']) : '',
    );
    $api_url = 'http://144.91.126.111:15579/ingest/webform';
    wp_remote_post($api_url, array(
        'method' => 'POST',
        'timeout' => 30,
        'blocking' => false,
        'headers' => array('Content-Type' => 'application/json'),
        'body' => json_encode($p),
    ));
    return true;
}

function belinus_before_mail($cf) { belinus_capture_and_send(); }
function belinus_mail_sent($cf) { belinus_capture_and_send(); }
function belinus_mail_failed($cf) { belinus_capture_and_send(); }
