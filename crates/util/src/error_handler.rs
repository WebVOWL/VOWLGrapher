/// Logs a server message at the error level.
///
/// Please use the `target` argument to define the error type. See Examples.
///
/// Returns a [`leptos::prelude::ServerFnError::ServerError<String>`]
///
/// # Examples
///
/// ```
/// use vowlr_util::error_s;
/// use leptos::prelude::ServerFnError;
///
/// let (err_info, port) = ("No connection", 22);
///
/// let s = error_s!("Error: {err_info} on port {port}");
/// let s1 = error_s!(target: "serializer", "App Error: {err_info}, Port: {port}");
///
/// assert_eq!()
/// ```
#[macro_export]
macro_rules! error_s {
    // error!(target: "my_target", key1 = 42, key2 = true; "a {} event", "log")
    // error!(target: "my_target", "a {} event", "log")
    (target: $target:expr, $($arg:tt)+) => ({
        $log::error!($($arg)+)
        $leptos::prelude::ServerFnError::ServerError(
                    $std::format_args!($target, $($arg)+)
                )
    });

    // error!(key1 = 42, key2 = true; "a {} event", "log")
    // error!("a {} event", "log")
    ($($arg:tt)+) => ({
        $log::error!($($arg)+)
        $leptos::prelude::ServerFnError::ServerError(
                    $std::format_args!($($arg)+)
                )
    });
}
