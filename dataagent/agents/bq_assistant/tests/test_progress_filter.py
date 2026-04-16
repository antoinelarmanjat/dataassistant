"""Tests for the frontend progress message filtering logic.

The filtering is implemented in the Vite bundle (contact.ts), but we replicate
the exact same heuristic here in Python to verify its correctness and serve as
a specification for the filtering rules.
"""

# Replicate the frontend filtering logic in Python
PROGRESS_EMOJIS = [
    "🔍", "📚", "🔬", "✅", "🚀", "🔄", "📊", "🔎", "🧠",
    "📤", "💾", "🌐", "📥", "📝", "📂", "🤔", "📋",
]
MAX_PROGRESS_LENGTH = 120


def is_progress_message(text: str) -> bool:
    """Replicate the frontend progress filtering heuristic."""
    stripped = text.strip()
    if len(stripped) > MAX_PROGRESS_LENGTH:
        return False
    return any(stripped.startswith(emoji) for emoji in PROGRESS_EMOJIS)


class TestProgressFiltering:
    """Verify that progress messages are correctly identified and filtered."""

    def test_standard_progress_messages_filtered(self):
        """Common progress messages from sub-agents should be filtered."""
        messages = [
            "🔍 **Step 1/5:** Loading semantic knowledge for this dataset...",
            "📚 **Step 2/5:** Checking if similar questions were asked before...",
            "🔬 **Step 3/5:** Probing column values to avoid errors...",
            "✅ **Step 4/5:** Validating query syntax and estimating cost...",
            "🚀 **Step 5/5:** Executing the query...",
            "🔄 **Retrying:** Fixing the query based on error diagnosis...",
            "📂 Loading your workspace...",
            "🤔 Reviewing your datasets...",
            "📚 Loading your saved queries...",
            "📋 Loading dataset insights...",
            "🧠 Planning and executing query...",
            "🌐 Searching the web...",
        ]
        for msg in messages:
            assert is_progress_message(msg), f"Should filter: {msg!r}"

    def test_real_agent_responses_not_filtered(self):
        """Actual agent responses (with data) should NOT be filtered."""
        messages = [
            # Long response with table data
            "Here are the top 10 most popular start stations in 2023:\n\n| Station | Trips |\n|---|---|" + "\n" * 10,
            # Multi-paragraph response
            "The data shows that Zilker Park was the most popular station. " * 5,
            # Greeting message
            "Hello! 👋\n\nI'm your **Data Assistant**. Here's what I can help you with:\n" + "- Feature\n" * 10,
        ]
        for msg in messages:
            assert not is_progress_message(msg), f"Should NOT filter: {msg[:60]!r}..."

    def test_emoji_in_long_response_not_filtered(self):
        """An emoji at the start of a long response should NOT be filtered."""
        long_msg = "📊 " + "data " * 50  # Well over 120 chars
        assert not is_progress_message(long_msg)

    def test_non_progress_emoji_not_filtered(self):
        """Emojis not in the progress set should NOT be filtered."""
        assert not is_progress_message("👋 Hello!")
        assert not is_progress_message("❌ Error!")
        assert not is_progress_message("🎉 Success!")

    def test_length_boundary(self):
        """Messages exactly at the 120-char boundary."""
        # 119 chars with progress emoji → should filter
        short = "🔍 " + "x" * 115  # emoji is 4 bytes but 1 char + space + 115 = 117 chars
        assert is_progress_message(short)

        # 121+ chars with progress emoji → should NOT filter
        long = "🔍 " + "x" * 120
        assert not is_progress_message(long)

    def test_whitespace_handling(self):
        """Leading/trailing whitespace shouldn't affect filtering."""
        assert is_progress_message("  🔍 Loading...  ")
        assert is_progress_message("\n📚 Loading queries...\n")

    def test_empty_string(self):
        """Empty strings should not be filtered as progress."""
        assert not is_progress_message("")
        assert not is_progress_message("   ")
