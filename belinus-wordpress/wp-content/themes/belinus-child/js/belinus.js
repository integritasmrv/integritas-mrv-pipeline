/**
 * belinus.js — Belinus Child Theme JS
 * Initializes Chatwoot (chat.belinus.net) and Cal.com booking widget.
 * No jQuery dependency.
 */
(function () {
  'use strict';

  /* --------------------------------------------------------------------
     CHATWOOT
     -------------------------------------------------------------------- */
  function initChatwootSDK() {
    var token = (window.BELINUS && window.BELINUS.chatwootToken) || '';
    if (!token) return;
    if (window.chatwootSDK) {
      window.chatwootSDK.run({
        position: 'right',
        type: 'standard',
        token: token,
      });
    } else {
      window.chatwootSettings = {
        position: 'right',
        type: 'standard',
        token: token,
      };
      var w = window;
      var i = function () {
        if (w.chatwootSDK) {
          w.chatwootSDK.run(w.chatwootSettings);
        }
      };
      var d = document;
      var g = d.createElement('script');
      g.src = 'https://cdn.chatwoot.com/sdks/woot.js';
      g.async = true;
      g.onload = i;
      d.head.appendChild(g);
    }
  }

  function initChatwootLinks() {
    document.querySelectorAll('[data-chatwoot-open]').forEach(function (el) {
      el.addEventListener('click', function (e) {
        e.preventDefault();
        if (window.chatwootSDK) {
          window.chatwootSDK.toggle();
        }
      });
    });
  }

  /* --------------------------------------------------------------------
     CAL.COM BOOKING
     -------------------------------------------------------------------- */
  function initCalCom() {
    var slug = (window.BELINUS && window.BELINUS.calcomSlug) || '';
    if (!slug) return;
    // Cal.com embed snippet — idempotent, only runs once
    if (document.querySelector('script[src*="cal.com/embed"]')) return;
    (function (C, A, L) {
      var p = function (a, ar) {
        p.q.push(a);
        if (!ar) p.q.concat(ar);
      };
      C[L] = C[L] || { q: [], init: function () { p(C[L], arguments); }, h: C[L].h };
      p(A, L);
    })(window, slug, 'Cal');
    var link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = 'https://cal.com/embed/embed.css';
    document.head.appendChild(link);
    (function (d, t) {
      var s = d.createElement('script');
      s.src = 'https://app.cal.com/embed/embed.js';
      s.async = true;
      var g = d.getElementsByTagName(t)[0];
      g.parentNode.insertBefore(s, g);
    })(document, 'script');
    Cal('init', { origin: 'https://cal.com' });
  }

  /* --------------------------------------------------------------------
     ROI CALCULATOR — GSAP micro-interaction
     -------------------------------------------------------------------- */
  function initRoiCalculator() {
    if (typeof gsap === 'undefined') return;
    var target = document.querySelector('[data-out="savings"]');
    var sliders = document.querySelectorAll('.bl-roi-slider');
    if (!target || !sliders.length) return;
    sliders.forEach(function (slider) {
      slider.addEventListener('input', function () {
        gsap.fromTo(
          target,
          { scale: 1 },
          { scale: 1.04, duration: 0.12, yoyo: true, repeat: 1, ease: 'power2.out' }
        );
      }, { passive: true });
    });
  }

  /* --------------------------------------------------------------------
     INIT
     -------------------------------------------------------------------- */
  function init() {
    initChatwootSDK();
    initChatwootLinks();
    initCalCom();
    initRoiCalculator();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
