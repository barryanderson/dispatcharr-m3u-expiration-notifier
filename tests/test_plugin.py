"""
Dependency-free tests for the pure logic in plugin.py.

Covers the pure helpers (`_parse_thresholds`, `_process_profile`,
`_format_hours`) plus plugin.json parity. Everything else touches the real
Django/ORM or a live background thread and can't be exercised here - see
CLAUDE.md for how to verify those against a live Dispatcharr container.

Run with:
    python3 -m unittest discover -s tests -v
"""

import datetime
import importlib.util
import json
import os
import unittest

_PLUGIN_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "plugin.py")
_spec = importlib.util.spec_from_file_location("plugin", _PLUGIN_PATH)
plugin = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(plugin)


_PLUGIN_JSON_PATH = os.path.join(os.path.dirname(_PLUGIN_PATH), "plugin.json")


class PluginJsonParityTests(unittest.TestCase):
    """plugin.json is read by Dispatcharr's loader without executing code
    (pre-enable discovery); once enabled, Plugin.fields/Plugin.actions in
    plugin.py take priority. The two must stay in sync - see CLAUDE.md."""

    def setUp(self):
        with open(_PLUGIN_JSON_PATH, encoding="utf-8") as fh:
            self.plugin_json = json.load(fh)

    def test_fields_are_byte_identical(self):
        self.assertEqual(plugin.Plugin.fields, self.plugin_json["fields"])

    def test_version_matches(self):
        # A version bump has to land in both files, or Dispatcharr's loader
        # and the running code disagree about the installed version.
        self.assertEqual(plugin.Plugin.version, self.plugin_json["version"])

    def test_actions_are_byte_identical(self):
        self.assertEqual(plugin.Plugin.actions, self.plugin_json["actions"])


class FakeProfile:
    def __init__(self, exp_date, profile_id=1):
        self.id = profile_id
        self.exp_date = exp_date


class ParseThresholdsTests(unittest.TestCase):
    def test_basic_list_sorted_descending_and_deduped(self):
        self.assertEqual(
            plugin._parse_thresholds("30,14,7,3,1"),
            [30, 14, 7, 3, 1],
        )
        self.assertEqual(
            plugin._parse_thresholds("1,3,3,7,7,7"),
            [7, 3, 1],
        )

    def test_whitespace_and_float_strings_are_tolerated(self):
        self.assertEqual(
            plugin._parse_thresholds(" 30 , 14.0, 7.9 "),
            [30, 14, 7],
        )

    def test_garbage_entries_are_skipped(self):
        self.assertEqual(
            plugin._parse_thresholds("30,abc,,14"),
            [30, 14],
        )

    def test_negative_values_are_excluded(self):
        self.assertEqual(
            plugin._parse_thresholds("30,-5,14"),
            [30, 14],
        )

    def test_empty_or_all_invalid_falls_back_to_default(self):
        default_thresholds = sorted(
            (int(day) for day in plugin.DEFAULTS["warning_days"].split(",")), reverse=True
        )
        self.assertEqual(plugin._parse_thresholds(""), default_thresholds)
        self.assertEqual(plugin._parse_thresholds("abc,def"), default_thresholds)

    def test_large_far_future_threshold(self):
        # Thresholds beyond the default list, for accounts expiring over a year out.
        self.assertEqual(
            plugin._parse_thresholds("1024,386,30,14,7,3,1"),
            [1024, 386, 30, 14, 7, 3, 1],
        )


class FormatHoursTests(unittest.TestCase):
    def test_whole_hours_have_no_decimal_point(self):
        self.assertEqual(plugin._format_hours(24.0), "24")
        self.assertEqual(plugin._format_hours(1), "1")

    def test_fractional_hours_keep_their_decimals(self):
        self.assertEqual(plugin._format_hours(1.5), "1.5")
        self.assertEqual(plugin._format_hours(0.25), "0.25")


class ProcessProfileTests(unittest.TestCase):
    NOW = datetime.datetime(2026, 7, 12, tzinfo=datetime.timezone.utc)
    THRESHOLDS = [1024, 386, 30, 14, 7, 3, 1]

    def _exp_in(self, days):
        return self.NOW + datetime.timedelta(days=days)

    def test_no_threshold_crossed_yet(self):
        profile = FakeProfile(self._exp_in(2000))
        state = {}
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertFalse(should_notify)
        self.assertIsNone(crossed)

    def test_first_notification_fires_for_furthest_matching_threshold(self):
        # ~258 days out -> should match the 386 bucket, not 30/14/7/3/1.
        profile = FakeProfile(self._exp_in(258))
        state = {}
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertTrue(should_notify)
        self.assertEqual(crossed, 386)

    def test_expired_account_always_crosses_zero(self):
        profile = FakeProfile(self._exp_in(-5))
        state = {}
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertTrue(should_notify)
        self.assertEqual(crossed, 0)

    def test_does_not_renotify_same_bucket(self):
        profile = FakeProfile(self._exp_in(258))
        state = {
            "1": {"exp_date": profile.exp_date.isoformat(), "last_notified_days": 386}
        }
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertFalse(should_notify)
        self.assertEqual(crossed, 386)

    def test_renotifies_on_more_urgent_threshold(self):
        # Already notified at 386; now within the 30-day window too.
        profile = FakeProfile(self._exp_in(25))
        state = {
            "1": {"exp_date": profile.exp_date.isoformat(), "last_notified_days": 386}
        }
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertTrue(should_notify)
        self.assertEqual(crossed, 30)

    def test_exp_date_change_resets_tracking(self):
        # Renewal: recorded exp_date differs, so tracking resets and the
        # current threshold fires again.
        profile = FakeProfile(self._exp_in(258), profile_id=2)
        state = {
            "2": {"exp_date": self._exp_in(-5).isoformat(), "last_notified_days": 0}
        }
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertTrue(should_notify)
        self.assertEqual(crossed, 386)
        self.assertEqual(state["2"]["exp_date"], profile.exp_date.isoformat())

    def test_stale_threshold_below_current_config_suppresses_notification(self):
        # last_notified_days holds 385, which is no longer configured. As
        # 385 < 386 the 386 bucket counts as already covered and must not
        # re-fire - this is the case Reset Notification State exists for.
        profile = FakeProfile(self._exp_in(258), profile_id=3)
        state = {
            "3": {"exp_date": profile.exp_date.isoformat(), "last_notified_days": 385}
        }
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertFalse(should_notify)
        self.assertEqual(crossed, 386)

    def test_reset_state_clears_stale_suppression(self):
        # Same as above but after a state reset - the notification fires.
        profile = FakeProfile(self._exp_in(258), profile_id=3)
        state = {}
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertTrue(should_notify)
        self.assertEqual(crossed, 386)

    def test_process_profile_does_not_mutate_last_notified_itself(self):
        # Only _do_check persists last_notified_days, and only after a
        # successful send - guards against marking a profile "notified"
        # before the email actually went out.
        profile = FakeProfile(self._exp_in(258), profile_id=4)
        state = {}
        should_notify, days_left, crossed = plugin._process_profile(
            profile, self.THRESHOLDS, self.NOW, state
        )
        self.assertTrue(should_notify)
        self.assertIsNone(state["4"]["last_notified_days"])


if __name__ == "__main__":
    unittest.main()
