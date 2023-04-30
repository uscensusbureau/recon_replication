#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)
#
# Simson's Sharepoint implementation


# This is how you can read from sharepoint with Windows Domain authentication.
# This is a stub


def sharepoint_broken() -> None:
    import win32com.client

    url = 'https://....'

    h = win32com.client.Dispatch('WinHTTP.WinHTTPRequest.5.1')
    h.SetAutoLogonPolicy(0)
    h.Open('GET', url, False)
    h.Send()
    result = h.responseText
