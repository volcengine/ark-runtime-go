// Copyright (c) 2026 ByteDance Ltd. and/or its affiliates.
// SPDX-License-Identifier: Apache-2.0

package markdown

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestImagesOnlySelectRenderableReferences(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		want       []string
	}{
		{"inline", "前![图](/mnt/session/outputs/a.png)后", []string{"/mnt/session/outputs/a.png"}},
		{"escaped parentheses", `![图](/mnt/session/outputs/a\(1\).png "标题")`, []string{"/mnt/session/outputs/a(1).png"}},
		{"angle destination", `![图](</mnt/session/outputs/a b.png>)`, []string{"/mnt/session/outputs/a b.png"}},
		{"nested label", `![图 [一]](/mnt/session/outputs/a(1).png)`, []string{"/mnt/session/outputs/a(1).png"}},
		{"inline code", "`![例](bad)` ![图](good)", []string{"good"}},
		{"long code delimiter", "``![例](`bad`)`` ![图](good)", []string{"good"}},
		{"fenced code", "```md\n![例](bad)\n```\n![图](good)", []string{"good"}},
		{"tilde fenced code", "~~~md\n![例](bad)\n~~~\n![图](good)", []string{"good"}},
		{"quoted tilde fenced code", "> ~~~md\n> ![例](bad)\n> ~~~\n![图](good)", []string{"good"}},
		{"quoted fence ends with quote", "> ~~~md\n> ![例](bad)\n\n![图](good)", []string{"good"}},
		{"escaped image", `\![例](bad)`, nil},
		{"unclosed", `![图](/mnt/session`, nil},
		{"parenthesis in title", `![图](bad "open (") tail`, []string{"bad"}},
		{"angle unmatched parenthesis", `![图](<bad(unclosed>) tail`, []string{"bad(unclosed"}},
		{"reference", "![图][ref]\n\n[ref]: https://example.test/a.png", []string{""}},
		{"shortcut reference", "![图]\n\n[图]: /mnt/session/outputs/a.png", []string{""}},
		{"indented code", "    ![例](bad)\n\t![例](bad)\n![图](good)", []string{"good"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, image := range Images(tc.text) {
				got = append(got, image.Destination)
				require.Equal(t, tc.text[image.Start:image.End], image.Text)
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestQuotePreservesImagesAndCodeExamples(t *testing.T) {
	text := "前 ![图 `一`](/mnt/session/outputs/a.png) 后\n`![示例](keep)`"
	want := "前 ``![图 `一`](/mnt/session/outputs/a.png)`` 后\n`![示例](keep)`"
	got := ReplaceImages(text, func(image Image) string { return image.Code() })
	require.Equal(t, want, got)
	require.Empty(t, Images(got), "回退引用不能再次进入图片解析")
	require.Equal(t, got, ReplaceImages(got, func(image Image) string { return image.Code() }))
}

func TestPrefixEndKeepsImageAndCodeAtomic(t *testing.T) {
	for _, text := range []string{
		"前![图](img_fixture)后",
		"前`![图](/mnt/session/outputs/a.png)`后",
		"前``![图 `一`](bad)``后",
	} {
		for end := 1; end <= utf8.RuneCountInString(text); end++ {
			next := PrefixEnd(text, end)
			require.GreaterOrEqual(t, next, end)
			prefix := string([]rune(text)[:next])
			if strings.Contains(prefix, "![") {
				require.Contains(t, prefix, ")", "图片与代码引用不能切到半截")
			}
			if strings.Contains(text, "`!") || strings.Contains(text, "``!") {
				require.NotEqual(t, "前`", prefix)
				require.NotEqual(t, "前``", prefix)
			}
		}
	}
}

func TestQuotedImagesCannotMatchExistingUnclosedBackticks(t *testing.T) {
	text := "` prefix ![a](bad) tail"
	got := ReplaceImages(text, func(image Image) string { return image.Code() })
	require.Equal(t, "` prefix ``![a](bad)`` tail", got)
	require.Empty(t, Images(got))
}

func TestImageReplacementPreservesTitle(t *testing.T) {
	text := `![图](</mnt/session/outputs/a b.png> "原始标题")`
	got := ReplaceImages(text, func(image Image) string { return image.WithDestination("img_fixture") })
	require.Equal(t, `![图](img_fixture "原始标题")`, got)
}
