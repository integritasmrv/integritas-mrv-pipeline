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
     GSAP SCROLL ANIMATIONS
     -------------------------------------------------------------------- */
  function initScrollAnimations() {
    if (typeof gsap === 'undefined') return;
    var reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    // Hero pageLoad — stagger each element in sequence
    var heroEyebrow = document.querySelector('.bl-hero__eyebrow');
    var heroHeadline = document.querySelector('.bl-hero__headline');
    var heroSubhead = document.querySelector('.bl-hero__subhead');
    var heroCTAs = document.querySelector('.bl-hero .bl-btn-primary, .bl-hero .bl-btn-secondary, .bl-hero__ctas');
    var heroTrust = document.querySelector('.bl-hero__trust');
    var heroVideo = document.querySelector('.bl-hero__bg-video');

    if (reduceMotion) {
      // Respect reduced motion — make everything visible immediately
      [heroEyebrow, heroHeadline, heroSubhead, heroCTAs, heroTrust].forEach(function(el){
        if (el) { el.style.opacity = '1'; el.style.transform = 'none'; }
      });
      return;
    }

    // Hero entrance timeline
    var tl = gsap.timeline({ defaults: { ease: 'power3.out' } });
    if (heroVideo) { tl.fromTo(heroVideo, { opacity: 0 }, { opacity: 0.45, duration: 1.5 }, 0); }
    if (heroEyebrow)  { tl.fromTo(heroEyebrow, { opacity: 0, y: 20 }, { opacity: 1, y: 0, duration: 0.6 }, 0.3); }
    if (heroHeadline) { tl.fromTo(heroHeadline, { opacity: 0, y: 30 }, { opacity: 1, y: 0, duration: 0.8 }, 0.5); }
    if (heroSubhead)  { tl.fromTo(heroSubhead, { opacity: 0, y: 20 }, { opacity: 1, y: 0, duration: 0.6 }, 0.8); }
    if (heroCTAs)     { tl.fromTo(heroCTAs, { opacity: 0, y: 20 }, { opacity: 1, y: 0, duration: 0.6 }, 1.0); }
    if (heroTrust)    { tl.fromTo(heroTrust, { opacity: 0, y: 20 }, { opacity: 1, y: 0, duration: 0.6 }, 1.3); }

    // Hero parallax — video scrolls slower than page
    if (heroVideo && typeof ScrollTrigger !== 'undefined') {
      gsap.to(heroVideo, {
        yPercent: 20,
        ease: 'none',
        scrollTrigger: {
          trigger: '.bl-hero',
          start: 'top top',
          end: 'bottom top',
          scrub: true
        }
      });
    }

    // Scroll-triggered section fades
    var sections = document.querySelectorAll('.wp-block-group.alignfull');
    sections.forEach(function(section) {
      // Skip the hero section (already animated)
      if (section.classList.contains('bl-hero')) return;
      // Skip if already visible (above fold)
      var rect = section.getBoundingClientRect();
      if (rect.top < window.innerHeight && rect.bottom > 0) return;

      var elements = section.querySelectorAll('h1,h2,h3,p,div[class*="bl-"],div[style*="flex"]');
      if (!elements.length) return;

      gsap.set(elements, { opacity: 0, y: 28 });
      gsap.to(elements, {
        opacity: 1,
        y: 0,
        duration: 0.7,
        ease: 'power2.out',
        stagger: 0.08,
        scrollTrigger: {
          trigger: section,
          start: 'top 82%',
          toggleActions: 'play none none reverse'
        }
      });
    });

    // Stats numbers — count up animation
    var statNumbers = document.querySelectorAll('.bl-stat-card__number');
    if (statNumbers.length) {
      gsap.set(statNumbers, { opacity: 0, y: 16 });
      gsap.to(statNumbers, {
        opacity: 1,
        y: 0,
        duration: 0.5,
        stagger: 0.12,
        scrollTrigger: {
          trigger: statNumbers[0].closest('.wp-block-group') || statNumbers[0],
          start: 'top 80%',
          toggleActions: 'play none none reverse'
        }
      });
    }

    // Value prop and use case cards — subtle lift on scroll
    var cards = document.querySelectorAll('.bl-valueprop-card, .bl-usecase-card');
    if (cards.length) {
      gsap.set(cards, { opacity: 0, y: 24 });
      gsap.to(cards, {
        opacity: 1,
        y: 0,
        duration: 0.55,
        ease: 'power2.out',
        stagger: 0.1,
        scrollTrigger: {
          trigger: cards[0].closest('.wp-block-group'),
          start: 'top 78%',
          toggleActions: 'play none none reverse'
        }
      });
    }

    // Footer columns fade in
    var footerCols = document.querySelectorAll('.wp-block-group.alignwide > div > div');
    if (footerCols.length) {
      gsap.set(footerCols, { opacity: 0, y: 12 });
      gsap.to(footerCols, {
        opacity: 1,
        y: 0,
        duration: 0.5,
        ease: 'power2.out',
        stagger: 0.1,
        scrollTrigger: {
          trigger: footerCols[0].closest('.wp-block-group'),
          start: 'top 90%',
          toggleActions: 'play none none reverse'
        }
      });
    }

    // Refresh ScrollTrigger after all animations set up
    if (typeof ScrollTrigger !== 'undefined') {
      ScrollTrigger.refresh();
    }
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
    // GSAP animations run after a brief delay to ensure DOM is ready
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', function() { setTimeout(initScrollAnimations, 50); });
    } else {
      setTimeout(initScrollAnimations, 50);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
