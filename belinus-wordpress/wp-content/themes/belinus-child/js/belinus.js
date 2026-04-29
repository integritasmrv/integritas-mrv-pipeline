/*!
 * Belinus Child Theme — main JS bundle
 *
 * Vanilla JS only. No dependencies. All in IIFE. DOMContentLoaded-guarded.
 * Modules:
 *   1. Nav scroll toggle
 *   2. Mobile drawer
 *   3. Accordion
 *   4. Stat counter (count-up on viewport entry)
 *   5. ROI calculator
 *   6. Reveal-up (IntersectionObserver fallback for Motion.page)
 *   7. Cal.com skeleton swap
 *
 * Reduced-motion preference is honored throughout.
 *
 * @version 1.3.0
 */

(function () {
	'use strict';

	const REDUCED_MOTION = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

	/* =====================================================================
	 * Helpers
	 * ===================================================================== */

	const $ = (sel, ctx) => (ctx || document).querySelector(sel);
	const $$ = (sel, ctx) => Array.from((ctx || document).querySelectorAll(sel));

	const onReady = (fn) => {
		if (document.readyState === 'loading') {
			document.addEventListener('DOMContentLoaded', fn, { once: true });
		} else {
			fn();
		}
	};

	const formatEUR = (n) =>
		new Intl.NumberFormat('en-IE', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
			.format(Math.max(0, Math.round(n)));

	const formatNumber = (n) =>
		new Intl.NumberFormat('en-IE', { maximumFractionDigits: 0 }).format(Math.max(0, Math.round(n)));

	/* =====================================================================
	 * 1. Nav scroll toggle
	 * ===================================================================== */

	const initNav = () => {
		const nav = $('.bl-nav');
		if (!nav) return;

		const SCROLL_THRESHOLD = 8;
		let ticking = false;

		const onScroll = () => {
			if (ticking) return;
			window.requestAnimationFrame(() => {
				const scrolled = window.scrollY > SCROLL_THRESHOLD;
				nav.classList.toggle('bl-nav--scrolled', scrolled);
				ticking = false;
			});
			ticking = true;
		};

		onScroll();
		window.addEventListener('scroll', onScroll, { passive: true });
	};

	/* =====================================================================
	 * 2. Mobile drawer
	 * ===================================================================== */

	const initDrawer = () => {
		const nav = $('.bl-nav');
		const toggle = $('.bl-nav__toggle', nav);
		const drawer = $('.bl-nav__drawer', nav);
		if (!nav || !toggle || !drawer) return;

		const open = () => {
			nav.setAttribute('aria-expanded', 'true');
			toggle.setAttribute('aria-expanded', 'true');
			document.documentElement.style.overflow = 'hidden';
			drawer.querySelector('a, button')?.focus({ preventScroll: true });
		};

		const close = () => {
			nav.setAttribute('aria-expanded', 'false');
			toggle.setAttribute('aria-expanded', 'false');
			document.documentElement.style.overflow = '';
			toggle.focus({ preventScroll: true });
		};

		toggle.setAttribute('aria-expanded', 'false');
		toggle.setAttribute('aria-controls', drawer.id || 'bl-nav-drawer');
		if (!drawer.id) drawer.id = 'bl-nav-drawer';

		toggle.addEventListener('click', () => {
			const isOpen = nav.getAttribute('aria-expanded') === 'true';
			isOpen ? close() : open();
		});

		// Close on Escape, on link click inside drawer, on outside click.
		document.addEventListener('keydown', (e) => {
			if (e.key === 'Escape' && nav.getAttribute('aria-expanded') === 'true') close();
		});

		drawer.addEventListener('click', (e) => {
			if (e.target.closest('a')) close();
		});
	};

	/* =====================================================================
	 * 3. Accordion
	 * ===================================================================== */

	const initAccordion = () => {
		$$('.bl-accordion').forEach((accordion) => {
			const items = $$('.bl-accordion__item', accordion);

			items.forEach((item) => {
				const header = $('.bl-accordion__header', item);
				if (!header) return;

				if (!item.hasAttribute('aria-expanded')) {
					item.setAttribute('aria-expanded', 'false');
				}
				header.setAttribute('role', 'button');
				header.setAttribute('tabindex', '0');

				const toggle = () => {
					const isOpen = item.getAttribute('aria-expanded') === 'true';
					item.setAttribute('aria-expanded', isOpen ? 'false' : 'true');
					header.setAttribute('aria-expanded', isOpen ? 'false' : 'true');
				};

				header.addEventListener('click', toggle);
				header.addEventListener('keydown', (e) => {
					if (e.key === 'Enter' || e.key === ' ') {
						e.preventDefault();
						toggle();
					}
				});
			});
		});
	};

	/* =====================================================================
	 * 4. Stat counter (count-up)
	 * ===================================================================== */

	const initStatCounters = () => {
		const counters = $$('.bl-counter');
		if (!counters.length) return;

		const animate = (el) => {
			const target = Number(el.dataset.target || el.textContent.replace(/[^\d.]/g, ''));
			const prefix = el.dataset.prefix || '';
			const suffix = el.dataset.suffix || '';
			const duration = Number(el.dataset.duration || 1500);

			if (REDUCED_MOTION || !Number.isFinite(target) || target <= 0) {
				el.textContent = prefix + formatNumber(target) + suffix;
				return;
			}

			const startTime = performance.now();

			const easeOutCubic = (t) => 1 - Math.pow(1 - t, 3);

			const tick = (now) => {
				const elapsed = now - startTime;
				const progress = Math.min(elapsed / duration, 1);
				const eased = easeOutCubic(progress);
				const current = target * eased;
				el.textContent = prefix + formatNumber(current) + suffix;

				if (progress < 1) {
					window.requestAnimationFrame(tick);
				} else {
					el.textContent = prefix + formatNumber(target) + suffix;
				}
			};

			window.requestAnimationFrame(tick);
		};

		const observer = new IntersectionObserver(
			(entries) => {
				entries.forEach((entry) => {
					if (entry.isIntersecting && !entry.target.dataset.animated) {
						entry.target.dataset.animated = '1';
						animate(entry.target);
					}
				});
			},
			{ threshold: 0.4 }
		);

		counters.forEach((el) => observer.observe(el));
	};

	/* =====================================================================
	 * 5. ROI calculator
	 *
	 * Formulas (from blueprint Phase 3 / page-spec section 6):
	 *   annual_savings = monthly_bill * peak_pct/100 * 12 * 0.35
	 *   payback_years  = (system_kwh * 450) / annual_savings
	 *   roi_10yr_pct   = ((annual_savings * 10 - system_kwh * 450) / (system_kwh * 450)) * 100
	 *
	 * Required HTML structure (from a Divi Code module):
	 *
	 *   <div class="bl-roi-calculator">
	 *     <div class="bl-roi-calculator__controls">
	 *       <label class="bl-roi-calculator__field">
	 *         <span class="bl-roi-calculator__label">
	 *           Monthly bill (€) <span class="bl-roi-calculator__value" data-out="bill">€5,000</span>
	 *         </span>
	 *         <input type="range" class="bl-roi-slider" data-input="bill"
	 *                min="500" max="50000" step="500" value="5000" />
	 *       </label>
	 *       ... peak, system ...
	 *     </div>
	 *     <div class="bl-roi-calculator__outputs">
	 *       <div class="bl-roi-calculator__output-block">
	 *         <span class="bl-roi-calculator__output-label">Annual savings</span>
	 *         <span class="bl-roi-calculator__output-value--primary" data-out="savings">€0</span>
	 *       </div>
	 *       ... payback, roi ...
	 *     </div>
	 *   </div>
	 * ===================================================================== */

	const initROI = () => {
		const widget = $('.bl-roi-calculator');
		if (!widget) return;

		const inputs = {
			bill:   $('input[data-input="bill"]', widget),
			peak:   $('input[data-input="peak"]', widget),
			system: $('input[data-input="system"]', widget),
		};
		const outputs = {
			bill:    $('[data-out="bill"]', widget),
			peak:    $('[data-out="peak"]', widget),
			system:  $('[data-out="system"]', widget),
			savings: $('[data-out="savings"]', widget),
			payback: $('[data-out="payback"]', widget),
			roi:     $('[data-out="roi"]', widget),
		};

		if (!inputs.bill || !inputs.peak || !inputs.system) return;

		const SYSTEM_COST_PER_KWH_EUR = 450;
		const PEAK_TO_SAVINGS_FACTOR = 0.35;

		const computeAndRender = () => {
			const monthlyBill = Number(inputs.bill.value);
			const peakPct = Number(inputs.peak.value);
			const systemKwh = Number(inputs.system.value);

			const annualSavings = monthlyBill * (peakPct / 100) * 12 * PEAK_TO_SAVINGS_FACTOR;
			const systemCost = systemKwh * SYSTEM_COST_PER_KWH_EUR;
			const paybackYears = annualSavings > 0 ? systemCost / annualSavings : Infinity;
			const tenYearROI = systemCost > 0 ? ((annualSavings * 10 - systemCost) / systemCost) * 100 : 0;

			// Live input echoes
			if (outputs.bill)   outputs.bill.textContent   = formatEUR(monthlyBill);
			if (outputs.peak)   outputs.peak.textContent   = peakPct + '%';
			if (outputs.system) outputs.system.textContent = formatNumber(systemKwh) + ' kWh';

			// Outputs
			if (outputs.savings) {
				outputs.savings.textContent = formatEUR(annualSavings);
				if (!REDUCED_MOTION) {
					outputs.savings.classList.remove('is-updating');
					// Force reflow so the animation can replay.
					void outputs.savings.offsetWidth;
					outputs.savings.classList.add('is-updating');
				}
			}
			if (outputs.payback) {
				outputs.payback.textContent = Number.isFinite(paybackYears)
					? paybackYears.toFixed(1) + ' years'
					: '—';
			}
			if (outputs.roi) {
				outputs.roi.textContent = formatNumber(tenYearROI) + ' %';
			}
		};

		Object.values(inputs).forEach((el) => {
			el.addEventListener('input', computeAndRender, { passive: true });
		});

		computeAndRender();
	};

	/* =====================================================================
	 * 6. Reveal-up (IntersectionObserver fallback)
	 *    Motion.page can intercept elements with .bl-reveal-up directly;
	 *    if it doesn't (or isn't loaded), we apply the .is-revealed class
	 *    on viewport entry so the CSS transition runs.
	 * ===================================================================== */

	const initReveal = () => {
		const targets = $$('.bl-reveal-up');
		if (!targets.length) return;

		// Don't fight Motion.page if it's running.
		if (window.MotionPage && typeof window.MotionPage.attached === 'function') {
			return;
		}

		if (REDUCED_MOTION) {
			targets.forEach((el) => el.classList.add('is-revealed'));
			return;
		}

		const observer = new IntersectionObserver(
			(entries) => {
				entries.forEach((entry) => {
					if (entry.isIntersecting) {
						entry.target.classList.add('is-revealed');
						observer.unobserve(entry.target);
					}
				});
			},
			{ threshold: 0.15, rootMargin: '0px 0px -10% 0px' }
		);

		targets.forEach((el) => observer.observe(el));
	};

	/* =====================================================================
	 * 7. Chatwoot chat-open links — any [data-chatwoot-open] anchor opens
	 *    the widget instead of navigating. Inline onclick handlers were
	 *    stripped by the page builder's security filter, so we wire them here.
	 * ===================================================================== */

	const initChatwootLinks = () => {
		const links = $$('[data-chatwoot-open]');
		if (!links.length) return;
		links.forEach((el) => {
			el.addEventListener('click', (e) => {
				if (window.chatwootSDK && typeof window.chatwootSDK.toggle === 'function') {
					e.preventDefault();
					window.chatwootSDK.toggle();
				}
			});
		});
	};

	/* =====================================================================
	 * 8. Cal.com skeleton swap
	 * ===================================================================== */

	const initCalcomSkeleton = () => {
		const wrap = $('.bl-schedule');
		if (!wrap) return;

		const skeleton = $('.bl-schedule__skeleton', wrap);
		const embed = $('.bl-schedule__embed', wrap);
		if (!skeleton || !embed) return;

		// When Cal.com posts its iframe ready event, swap.
		const onReadyMsg = (e) => {
			if (!e.data || typeof e.data !== 'object') return;
			if (e.data.type === 'CAL:ready' || e.data?.action === 'CAL:ready') {
				skeleton.style.display = 'none';
				embed.removeAttribute('hidden');
			}
		};
		window.addEventListener('message', onReadyMsg);

		// Hard fallback after 6 s.
		setTimeout(() => {
			if (skeleton.style.display !== 'none') {
				skeleton.style.display = 'none';
				embed.removeAttribute('hidden');
			}
		}, 6000);
	};

	/* =====================================================================
	 * Boot
	 * ===================================================================== */

	onReady(() => {
		initNav();
		initDrawer();
		initAccordion();
		initStatCounters();
		initROI();
		initReveal();
		initChatwootLinks();
		initCalcomSkeleton();
	});
})();
