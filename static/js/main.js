/**
 * Postly Landing Page Interactions
 * Buffer-inspired smooth animations and interactions
 */

// Smooth scroll for anchor links
document.addEventListener('DOMContentLoaded', function() {
    // Intersection Observer for fade-in animations
    const observerOptions = {
        threshold: 0.1,
        rootMargin: '0px 0px -50px 0px'
    };

    const observer = new IntersectionObserver(function(entries) {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.style.opacity = '1';
                entry.target.style.transform = 'translateY(0)';
                observer.unobserve(entry.target);
            }
        });
    }, observerOptions);

    // Observe all elements with fade-in classes
    const fadeElements = document.querySelectorAll('.fade-in, .fade-in-delay-1, .fade-in-delay-2, .fade-in-delay-3');
    fadeElements.forEach(el => {
        // Set initial state for non-hero elements
        if (!el.closest('.hero-section')) {
            el.style.opacity = '0';
            el.style.transform = 'translateY(20px)';
        }
        observer.observe(el);
    });

    // Smooth scroll for all anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function(e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });

    // Add active state to navigation on scroll
    const navbar = document.querySelector('.navbar');
    let lastScrollTop = 0;

    window.addEventListener('scroll', function() {
        const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        
        // Add shadow on scroll
        if (scrollTop > 10) {
            navbar.style.boxShadow = '0 4px 12px rgba(0, 0, 0, 0.08)';
        } else {
            navbar.style.boxShadow = '0 1px 3px rgba(0, 0, 0, 0.05)';
        }

        lastScrollTop = scrollTop;
    });

    // Email input validation with visual feedback
    const emailInputs = document.querySelectorAll('input[type="email"]');
    emailInputs.forEach(input => {
        input.addEventListener('blur', function() {
            if (this.value && !isValidEmail(this.value)) {
                this.style.borderColor = '#dc3545';
                
                // Remove error styling after user starts typing again
                this.addEventListener('input', function() {
                    this.style.borderColor = '';
                }, { once: true });
            }
        });
    });

    // Form validation helper
    function isValidEmail(email) {
        return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    }

    // Add ripple effect to buttons
    const buttons = document.querySelectorAll('.btn-primary, .btn-outline');
    buttons.forEach(button => {
        button.addEventListener('click', function(e) {
            const ripple = document.createElement('span');
            const rect = this.getBoundingClientRect();
            const size = Math.max(rect.width, rect.height);
            const x = e.clientX - rect.left - size / 2;
            const y = e.clientY - rect.top - size / 2;

            ripple.style.width = ripple.style.height = size + 'px';
            ripple.style.left = x + 'px';
            ripple.style.top = y + 'px';
            ripple.classList.add('ripple');

            this.style.position = 'relative';
            this.style.overflow = 'hidden';
            this.appendChild(ripple);

            setTimeout(() => ripple.remove(), 600);
        });
    });

    // Add CSS for ripple effect
    const style = document.createElement('style');
    style.textContent = `
        .ripple {
            position: absolute;
            border-radius: 50%;
            background: rgba(255, 255, 255, 0.6);
            transform: scale(0);
            animation: ripple-animation 0.6s ease-out;
            pointer-events: none;
        }
        
        @keyframes ripple-animation {
            to {
                transform: scale(2);
                opacity: 0;
            }
        }
        
        .btn-primary .ripple {
            background: rgba(255, 255, 255, 0.5);
        }
        
        .btn-outline .ripple {
            background: rgba(44, 75, 255, 0.2);
        }
    `;
    document.head.appendChild(style);

    // Platform icons hover effect
    const platformIcons = document.querySelectorAll('.platform-icon');
    platformIcons.forEach(icon => {
        icon.addEventListener('mouseenter', function() {
            this.style.transform = 'scale(1.1) rotate(5deg)';
            this.style.transition = 'transform 0.3s ease';
        });

        icon.addEventListener('mouseleave', function() {
            this.style.transform = 'scale(1) rotate(0deg)';
        });
    });

    // Testimonial cards parallax effect
    const testimonialCards = document.querySelectorAll('.testimonial-card');
    testimonialCards.forEach((card, index) => {
        card.addEventListener('mouseenter', function() {
            this.style.transform = 'translateY(-8px)';
            this.style.transition = 'transform 0.3s ease';
        });

        card.addEventListener('mouseleave', function() {
            this.style.transform = 'translateY(0)';
        });
    });

    // Feature cards stagger animation on scroll
    const featureCards = document.querySelectorAll('.feature-card');
    const featureObserver = new IntersectionObserver(function(entries) {
        entries.forEach((entry, index) => {
            if (entry.isIntersecting) {
                setTimeout(() => {
                    entry.target.style.opacity = '1';
                    entry.target.style.transform = 'translateY(0)';
                }, index * 100);
                featureObserver.unobserve(entry.target);
            }
        });
    }, observerOptions);

    featureCards.forEach(card => {
        card.style.opacity = '0';
        card.style.transform = 'translateY(30px)';
        card.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
        featureObserver.observe(card);
    });

    // Stats counter animation
    const statItems = document.querySelectorAll('.stat-item h3');
    const statsObserver = new IntersectionObserver(function(entries) {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const target = entry.target;
                const text = target.textContent;
                const hasPlus = text.includes('+');
                const hasM = text.includes('M');
                const number = parseInt(text.replace(/[^0-9]/g, ''));

                if (!isNaN(number)) {
                    animateNumber(target, 0, number, 1500, hasPlus, hasM);
                }
                statsObserver.unobserve(target);
            }
        });
    }, observerOptions);

    statItems.forEach(item => {
        statsObserver.observe(item);
    });

    function animateNumber(element, start, end, duration, hasPlus = false, hasM = false) {
        const startTime = performance.now();

        function update(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);

            // Easing function for smooth animation
            const easeOutQuart = 1 - Math.pow(1 - progress, 4);
            const current = Math.floor(start + (end - start) * easeOutQuart);

            let displayValue = current.toLocaleString();
            if (hasM && progress === 1) displayValue = end + 'M';
            if (hasPlus) displayValue += '+';

            element.textContent = displayValue;

            if (progress < 1) {
                requestAnimationFrame(update);
            }
        }

        requestAnimationFrame(update);
    }

    // Navbar mobile menu enhancements
    const navbarToggler = document.querySelector('.navbar-toggler');
    const navbarCollapse = document.querySelector('.navbar-collapse');

    if (navbarToggler && navbarCollapse) {
        navbarToggler.addEventListener('click', function() {
            if (navbarCollapse.classList.contains('show')) {
                navbarCollapse.style.animation = 'slideUp 0.3s ease';
            } else {
                navbarCollapse.style.animation = 'slideDown 0.3s ease';
            }
        });

        // Add slide animations
        const navStyle = document.createElement('style');
        navStyle.textContent = `
            @keyframes slideDown {
                from {
                    opacity: 0;
                    transform: translateY(-10px);
                }
                to {
                    opacity: 1;
                    transform: translateY(0);
                }
            }
            
            @keyframes slideUp {
                from {
                    opacity: 1;
                    transform: translateY(0);
                }
                to {
                    opacity: 0;
                    transform: translateY(-10px);
                }
            }
        `;
        document.head.appendChild(navStyle);
    }

    // Form input focus effects
    const formControls = document.querySelectorAll('.form-control');
    formControls.forEach(input => {
        input.addEventListener('focus', function() {
            this.parentElement.style.transform = 'scale(1.01)';
            this.parentElement.style.transition = 'transform 0.2s ease';
        });

        input.addEventListener('blur', function() {
            this.parentElement.style.transform = 'scale(1)';
        });
    });

    // Password strength indicator
    const passwordInputs = document.querySelectorAll('input[type="password"]');
    passwordInputs.forEach(input => {
        if (input.name === 'password') {
            input.addEventListener('input', function() {
                const strength = getPasswordStrength(this.value);
                updatePasswordStrengthUI(this, strength);
            });
        }
    });

    function getPasswordStrength(password) {
        let strength = 0;
        if (password.length >= 8) strength++;
        if (password.length >= 12) strength++;
        if (/[a-z]/.test(password) && /[A-Z]/.test(password)) strength++;
        if (/\d/.test(password)) strength++;
        if (/[^a-zA-Z0-9]/.test(password)) strength++;
        return strength;
    }

    function updatePasswordStrengthUI(input, strength) {
        let indicator = input.parentElement.querySelector('.password-strength');
        
        if (!indicator) {
            indicator = document.createElement('div');
            indicator.className = 'password-strength';
            indicator.style.cssText = 'height: 3px; margin-top: 0.5rem; border-radius: 2px; transition: all 0.3s ease;';
            input.parentElement.appendChild(indicator);
        }

        const colors = ['#dc3545', '#fd7e14', '#ffc107', '#28a745', '#20c997'];
        const widths = ['20%', '40%', '60%', '80%', '100%'];

        indicator.style.width = widths[strength - 1] || '0%';
        indicator.style.backgroundColor = colors[strength - 1] || '#e8e8e8';
    }
});

// Export for potential use in other scripts
if (typeof module !== 'undefined' && module.exports) {
    module.exports = {
        // Add any functions you want to export here
    };
}
