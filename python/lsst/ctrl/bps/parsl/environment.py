import os

__all__ = ("export_environment",)


def export_environment():
    """Generate bash script to regenerate the current environment"""
    output = ""
    for key, val in os.environ.items():
        if key in ("DISPLAY",):
            continue
        if val.startswith("() {"):
            # This is a function.
            # "Two parentheses, a single space, and a brace"
            # is exactly the same criterion as bash uses.

            # From 2014-09-25, the function name is prefixed by 'BASH_FUNC_'
            # and suffixed by '()', which we have to remove.
            if key.startswith("BASH_FUNC_") and key.endswith("()"):
                key = key[10:-2]

            output += "{key} {val}\nexport -f {key}\n".format(key=key, val=val)
        else:
            # This is a variable.
            output += "export {key}='{val}'\n".format(key=key, val=val.replace("'", "'\"'\"'"))
    return output
