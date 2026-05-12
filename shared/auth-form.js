// shared/auth-form.js
// Shared phone+PIN login form partial. Renders the canonical 4-box PIN grid
// into a container element on driver/collector/agent/aggregator dashboards
// so all four show the same form. Exposes element IDs (loginPhone, loginPin,
// loginError, loginBtn) that existing dashboard handlers already read.

window.CirculAuthForm = window.CirculAuthForm || (function () {
  var SUBTITLES = {
    driver: 'Driver access',
    collector: 'Collector access',
    agent: 'Agent access',
    aggregator: 'Aggregator access'
  };

  var FORGOT_HREF = 'mailto:jeffrey@thebolderstudio.com?subject=Forgot%20PIN%20%E2%80%94%20Circul';

  function renderPhonePinLoginForm(containerSelector, opts) {
    var container = typeof containerSelector === 'string'
      ? document.querySelector(containerSelector)
      : containerSelector;
    if (!container) return;
    opts = opts || {};
    var role = opts.role || 'collector';
    var subtitle = SUBTITLES[role] || (role.charAt(0).toUpperCase() + role.slice(1) + ' access');

    container.innerHTML =
      '<div class="login-title">Sign in to Circul</div>' +
      '<div class="login-sub">' + subtitle + '</div>' +
      '<div class="login-error" id="loginError"></div>' +
      '<div class="form-group" style="margin-bottom:16px">' +
        '<label class="form-label" for="loginPhone">Phone number</label>' +
        '<input class="form-input" id="loginPhone" type="tel" placeholder="0244 000 000" autocomplete="tel" inputmode="tel">' +
      '</div>' +
      '<div class="form-group" style="margin-bottom:6px">' +
        '<label class="form-label">4-digit PIN</label>' +
        '<div class="pin-row">' +
          '<input class="pin-box" type="password" id="pin1" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off" aria-label="PIN digit 1">' +
          '<input class="pin-box" type="password" id="pin2" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off" aria-label="PIN digit 2">' +
          '<input class="pin-box" type="password" id="pin3" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off" aria-label="PIN digit 3">' +
          '<input class="pin-box" type="password" id="pin4" maxlength="1" inputmode="numeric" pattern="[0-9]" autocomplete="off" aria-label="PIN digit 4">' +
        '</div>' +
        '<input type="hidden" id="loginPin">' +
      '</div>' +
      '<a class="login-forgot" href="' + FORGOT_HREF + '">Forgot your PIN?</a>' +
      '<button class="form-submit" id="loginBtn" type="button">Sign in</button>';

    var phoneInput = container.querySelector('#loginPhone');
    var pinBoxes = [
      container.querySelector('#pin1'),
      container.querySelector('#pin2'),
      container.querySelector('#pin3'),
      container.querySelector('#pin4')
    ];
    var pinHidden = container.querySelector('#loginPin');
    var btn = container.querySelector('#loginBtn');

    function getPin() {
      return pinBoxes.map(function (b) { return b.value; }).join('');
    }
    function syncHidden() {
      pinHidden.value = getPin();
    }

    pinBoxes.forEach(function (box, i) {
      box.addEventListener('input', function () {
        this.value = this.value.replace(/[^0-9]/g, '');
        syncHidden();
        if (this.value && i < 3) pinBoxes[i + 1].focus();
      });
      box.addEventListener('keydown', function (e) {
        if (e.key === 'Backspace' && !this.value && i > 0) pinBoxes[i - 1].focus();
        if (e.key === 'Enter') submit();
      });
    });

    phoneInput.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && pinBoxes[0]) pinBoxes[0].focus();
    });

    function submit() {
      syncHidden();
      var phone = phoneInput.value.trim();
      var pin = pinHidden.value;
      if (!phone || pin.length < 4) return;
      if (typeof opts.onSubmit === 'function') opts.onSubmit(phone, pin);
    }

    btn.addEventListener('click', submit);
  }

  return { renderPhonePinLoginForm: renderPhonePinLoginForm };
})();
