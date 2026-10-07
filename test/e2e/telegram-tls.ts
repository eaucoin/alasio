/**
 * What makes the end-to-end run's Telegram stand-in api.telegram.org to Grafana, whose
 * Telegram integration calls Telegram there, over HTTPS, and nowhere else: a CA made for
 * these tests, which Grafana is made to trust, and the stand-in's certificate for
 * api.telegram.org and its key, which the CA signed. Made once, valid for a century.
 */

/** The CA, which Grafana trusts in the run alone (./harness.ts). */
export const CA = `-----BEGIN CERTIFICATE-----
MIIBlzCCAT2gAwIBAgIUNs8BiB57ED5yrMkEZZRtERheDlcwCgYIKoZIzj0EAwIw
GDEWMBQGA1UEAwwNYWxhc2lvLWUyZS1jYTAgFw0yNjEwMDcwNDU1MDRaGA8yMTI2
MDkxMzA0NTUwNFowGDEWMBQGA1UEAwwNYWxhc2lvLWUyZS1jYTBZMBMGByqGSM49
AgEGCCqGSM49AwEHA0IABLoeQ2DhKgIEywClk97T90nsp00+KH5djfZm6pGnIEUw
Qr2V7xqrbX/Cen6RVMMEJeOv7yXg2owLs9wdbbBaFAqjYzBhMB0GA1UdDgQWBBSj
5Qd8VyUsHH1GnsK3fRbivcA0QzAfBgNVHSMEGDAWgBSj5Qd8VyUsHH1GnsK3fRbi
vcA0QzAPBgNVHRMBAf8EBTADAQH/MA4GA1UdDwEB/wQEAwICBDAKBggqhkjOPQQD
AgNIADBFAiEAnEZnT7yY6Zu0XBOTuFY6epq7J011/V8sbwvOQdpHcjMCIEwlbMuw
ARjULYIDREBMGF4bE9WqcNNpCRyv2HKC/iBS
-----END CERTIFICATE-----
`;

/** The stand-in's certificate, for api.telegram.org. */
export const CERTIFICATE = `-----BEGIN CERTIFICATE-----
MIIBtzCCAVygAwIBAgIUZmDeceTrTRSNpfott8nuSVi6CZ0wCgYIKoZIzj0EAwIw
GDEWMBQGA1UEAwwNYWxhc2lvLWUyZS1jYTAgFw0yNjEwMDcwNDU1MDRaGA8yMTI2
MDkxMzA0NTUwNFowGzEZMBcGA1UEAwwQYXBpLnRlbGVncmFtLm9yZzBZMBMGByqG
SM49AgEGCCqGSM49AwEHA0IABDi9d6SCHAbgIWjyZvoSGBSUQ3BID6guFnEillSE
vvAVWGEC7/a8UOGccT+R/B0OVqwFYRfPRvXnZ1HDTeBTq7SjfzB9MBsGA1UdEQQU
MBKCEGFwaS50ZWxlZ3JhbS5vcmcwEwYDVR0lBAwwCgYIKwYBBQUHAwEwCQYDVR0T
BAIwADAdBgNVHQ4EFgQUNe2uOaNTlThhsH84YthAsS8LtOQwHwYDVR0jBBgwFoAU
o+UHfFclLBx9Rp7Ct30W4r3ANEMwCgYIKoZIzj0EAwIDSQAwRgIhAKh+9tmL5qiY
G6FBVXtAi8MFuGS8z4ITqET1UXi5E6/DAiEAmS/Sfbjq2Ff5SNymcK49Cb1kJ8cH
ak7eWneSrPXGadU=
-----END CERTIFICATE-----
`;

/** Its key. */
export const KEY = `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg+Xen5JPsc8kW94uB
IsWgY8nWc2DRrdi+pYtSLPap/IyhRANCAAQ4vXekghwG4CFo8mb6EhgUlENwSA+o
LhZxIpZUhL7wFVhhAu/2vFDhnHE/kfwdDlasBWEXz0b152dRw03gU6u0
-----END PRIVATE KEY-----
`;
