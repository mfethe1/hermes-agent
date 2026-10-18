"""Deferred delivery callback ownership shared by turn completion paths."""

def pop_post_delivery_callback(adapter, key, generation):
    """Pop the adapter's deferred post-delivery callback for ``key`` (legacy dict fallback)."""
    if getattr(type(adapter), "pop_post_delivery_callback", None) is not None:
        return adapter.pop_post_delivery_callback(key, generation=generation)
    if adapter and hasattr(adapter, "_post_delivery_callbacks"):
        return adapter._post_delivery_callbacks.pop(key, None)
    return None
