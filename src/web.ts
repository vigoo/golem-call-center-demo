import { Http, HttpRouter } from "@golemcloud/effect-golem";

HttpRouter.define("Web", {
  mount: Http.mount("/", {
    fileResponseHeaders: {
      "Referrer-Policy": "no-referrer",
      "Content-Security-Policy": "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; worker-src 'self'; media-src 'self' blob:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
    },
  }),
  static: [
    { route: "/", path: "/web/index.html" },
    { route: "/walkthrough", path: "/web/walkthrough.html" },
    { route: "/assets/*", path: "/web/assets/$1" },
  ],
}).register();
