"""Custom Apache Beam Assert Matchers for Streaming and TestStream Verification.

Provides zero-cost assertion predicates for verifying unbounded PCollections,
set equality across sliding window firings, element counts, and structural invariants.
"""

from typing import Any, Callable, Collection, Iterable, Optional, Set


def matches_set(
    expected_set: Set[Any],
    key: Optional[Callable[[Any], Any]] = None,
) -> Callable[[Iterable[Any]], None]:
    """Return an assert_that matcher verifying unique elements in PCollection match expected set.

    Args:
        expected_set: The set of expected values.
        key: Optional transformation function applied to elements before set comparison.

    Returns:
        Matcher callable suitable for apache_beam.testing.util.assert_that.
    """
    def _matcher(actual: Iterable[Any]) -> None:
        if key is not None:
            actual_set = set(key(item) for item in actual)
        else:
            actual_set = set(actual)

        if actual_set != expected_set:
            missing = expected_set - actual_set
            unexpected = actual_set - expected_set
            raise AssertionError(
                f"matches_set assertion failed!\n"
                f"Expected: {expected_set}\n"
                f"Actual: {actual_set}\n"
                f"Missing: {missing}\n"
                f"Unexpected: {unexpected}"
            )

    return _matcher


def has_count(expected_count: int) -> Callable[[Iterable[Any]], None]:
    """Return an assert_that matcher verifying the exact number of elements in PCollection.

    Args:
        expected_count: Expected total count of emitted elements.

    Returns:
        Matcher callable for assert_that.
    """
    def _matcher(actual: Iterable[Any]) -> None:
        actual_list = list(actual)
        actual_count = len(actual_list)
        if actual_count != expected_count:
            raise AssertionError(
                f"has_count assertion failed! Expected {expected_count} elements, got {actual_count}."
            )

    return _matcher


def contains_subset(expected_subset: Collection[Any]) -> Callable[[Iterable[Any]], None]:
    """Return an assert_that matcher verifying that all elements in expected_subset are present.

    Args:
        expected_subset: Elements that must be contained in the actual PCollection.

    Returns:
        Matcher callable for assert_that.
    """
    def _matcher(actual: Iterable[Any]) -> None:
        actual_set = set(actual)
        missing = [item for item in expected_subset if item not in actual_set]
        if missing:
            raise AssertionError(
                f"contains_subset assertion failed! Missing elements: {missing}\nActual: {actual_set}"
            )

    return _matcher


def elements_satisfy(
    predicate: Callable[[Any], bool],
    description: str = "Predicate check failed",
) -> Callable[[Iterable[Any]], None]:
    """Return an assert_that matcher verifying every element satisfies a given predicate.

    Args:
        predicate: Function returning True if element passes.
        description: Diagnostic description on assertion failure.

    Returns:
        Matcher callable for assert_that.
    """
    def _matcher(actual: Iterable[Any]) -> None:
        actual_list = list(actual)
        failures = [item for item in actual_list if not predicate(item)]
        if failures:
            raise AssertionError(
                f"elements_satisfy failed: {description}.\n"
                f"Failing items ({len(failures)}/{len(actual_list)}): {failures[:5]}"
            )

    return _matcher
