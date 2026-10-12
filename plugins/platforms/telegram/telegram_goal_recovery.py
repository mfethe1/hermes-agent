"""Native, message-bound goal action cards; core callback owns validation and transitions."""
from __future__ import annotations

import contextlib
import re
import time


class TelegramGoalRecoveryMixin:
    @staticmethod
    def _rows_of_two(buttons: list) -> list:
        """Two buttons per row keep mobile labels readable."""
        return [buttons[i:i + 2] for i in range(0, len(buttons), 2)]

    async def send_goal_recovery_menu(self, chat_id, menu, on_selected, metadata=None):
        from .adapter import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, _escape_mdv2
        registry = getattr(self, "_goal_recovery_menus", None)
        if registry is None:
            registry = self._goal_recovery_menus = {}
        for key in [key for key, row in registry.items() if row["menu"]["expires_at"] <= time.time()]:
            registry.pop(key)
        if len(registry) >= 256:
            registry.pop(min(registry, key=lambda key: registry[key]["menu"]["expires_at"]))
        meta = dict(metadata or {})
        thread = str(meta["thread_id"]) if meta.get("thread_id") is not None else None
        def build():
            buttons = []
            for choice in menu["choices"]:
                data = f"gu:{menu['id']}:{choice['id']}"
                if len(data.encode()) > 64:
                    raise ValueError("goal callback exceeds Telegram's limit")
                buttons.append(InlineKeyboardButton(choice["label"], callback_data=data))
            keyboard = InlineKeyboardMarkup(self._rows_of_two(buttons))
            def remember(message):
                registry[(str(chat_id), str(message.message_id), menu["id"])] = dict(
                    menu=menu, callback=on_selected, thread=thread, metadata=meta, busy=False)
            # Menu text is plain prose; _send_prompt treats parse_mode=None as MarkdownV2, so escape every
            # reserved character or Telegram rejects the card ("character '.' is reserved").
            return _escape_mdv2(str(menu["text"])), keyboard, remember
        return await self._send_prompt("send_goal_recovery_menu", chat_id, meta, build,
                                       parse_mode=ParseMode.MARKDOWN_V2, thread_id=thread, reply_to_mode=self._reply_to_mode)

    async def _handle_goal_recovery_callback(self, query, data, cb):
        if not await self._callback_authorized(query, cb, "You are not authorized to control this goal."):
            return
        match = re.fullmatch(r"gu:([0-9a-f]{16}):([a-z][a-z0-9_-]{0,40})", str(data))
        if not match or not query.message:
            await query.answer(text="Invalid goal choice. Use /goal unblock.")
            return
        token, action = match.groups()
        key = (str(query.message.chat_id), str(query.message.message_id), token)
        entry = getattr(self, "_goal_recovery_menus", {}).get(key)
        thread = self._effective_message_thread_id(query.message)
        if (not entry or entry["menu"]["expires_at"] <= time.time() or thread != entry["thread"]
                or action not in {c["id"] for c in entry["menu"]["choices"]}):
            await query.answer(text="This goal menu expired or belongs to another topic. Use /goal unblock.")
            return
        if entry["busy"]:
            await query.answer(text="This choice is already being processed.")
            return
        chat = query.message.chat
        source = self.build_source(chat_id=str(query.message.chat_id), chat_name=getattr(chat, "title", None),
            chat_type=self._normalize_chat_type(getattr(chat, "type", "dm"), is_forum=getattr(chat, "is_forum", False) is True),
            user_id=str(query.from_user.id), user_name=getattr(query.from_user, "first_name", None),
            thread_id=thread, message_id=str(query.message.message_id), is_bot=bool(getattr(query.from_user, "is_bot", False)))
        entry["busy"] = True
        try:
            result = await entry["callback"](source, token, action)
        except (RuntimeError, ValueError, OSError):
            await query.answer(text="Could not apply this choice. Use /goal unblock to retry.")
            return
        finally:
            entry["busy"] = False
        toast = str(result.output).encode("utf-8")[:180].decode("utf-8", errors="ignore")
        await query.answer(text=toast)
        if result.error:
            if getattr(result, "menu", None):
                self._goal_recovery_menus.pop(key, None)
                with contextlib.suppress(Exception):
                    await query.edit_message_reply_markup(reply_markup=None)
                await self.send_goal_recovery_menu(source.chat_id, result.menu, entry["callback"], metadata=entry["metadata"])
            return
        if action != "details":
            self._goal_recovery_menus.pop(key, None)
            with contextlib.suppress(Exception):
                await query.edit_message_reply_markup(reply_markup=None)
        if result.output:
            await self.send(source.chat_id, result.output, metadata=entry["metadata"])
