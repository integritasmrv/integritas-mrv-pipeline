<?php
/**
 * Plugin Name: Belinus CF7 to API
 * Description: Forwards Contact Form 7 submissions to the Belinus API
 * Version: 1.0
 * Author: IntegritasMRV
 */

add_action('wpcf7_submit', 'belinus_forward_to_api', 10, 2);

function belinus_forward_to_api($contact_form, $result) {
    if ($result['status'] !== 'mail_sent') {
        return;
    }

    $submission = WPCF7_Submission::get_instance();
    if (!$submission) {
        return;
    }

    $data = $submission->get_posted_data();
    
    $payload = array(
        'first-name'   => isset($data['first-name']) ? sanitize_text_field($data['first-name']) : '',
        'last-name'    => isset($data['last-name']) ? sanitize_text_field($data['last-name']) : '',
        'your-email'   => isset($data['your-email']) ? sanitize_email($data['your-email']) : '',
        'phone'        => isset($data['phone']) ? sanitize_text_field($data['phone']) : '',
        'company'      => isset($data['company']) ? sanitize_text_field($data['company']) : '',
        'message'      => isset($data['message']) ? sanitize_textarea_field($data['message']) : '',
    );

    if (empty($payload['first-name']) && isset($data['first_name'])) {
        $payload['first-name'] = sanitize_text_field($data['first_name']);
    }
    if (empty($payload['last-name']) && isset($data['last_name'])) {
        $payload['last-name'] = sanitize_text_field($data['last_name']);
    }
    if (empty($payload['your-email']) && isset($data['email'])) {
        $payload['your-email'] = sanitize_email($data['email']);
    }

    $api_url = 'http://144.91.126.111:15579/ingest/webform';
    
    $response = wp_safe_remote_post($api_url, array(
        'method'      => 'POST',
        'timeout'     => 30,
        'redirection' => 5,
        'httpversion' => '1.0',
        'blocking'    => false,
        'headers'     => array('Content-Type' => 'application/json'),
        'body'        => json_encode($payload),
    ));

    error_log('[Belinus CF7] Forwarded to API');
}