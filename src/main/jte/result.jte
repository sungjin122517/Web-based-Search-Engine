@param com.comp4321.server.ResultPage page

<!DOCTYPE html>
<html lang="en">

<head>
    <title>Search Results</title>
</head>

<body>
    <h1>Search Results</h1>
    <h4>Search keywords: ${page.keywords().toString()}</h4>
    <h4>Search phrase: ${page.phrase().toString()}</h4>

    <ol>
        @for(final var result : page.results())
            <li>
                <h3>${String.format("%.4f", result.score())} <a href="${result.url()}">${result.title()}</a></h3>
                <a href="${result.url()}">${result.url()}</a>
                <p>${result.lastModified().toString()}, ${result.pageSize()}</p>
                <p>Keywords: ${result.topKeywordsToString()}</p>
                <p>Parent Links:</p>
                <ul>
                @for(final var link : result.parentLinks())
                    <li><a href="${link}">${link}</a></li>
                @endfor
                </ul>
                <p>Child Links:</p>
                <ul>
                @for(final var link : result.childLinks())
                    <li><a href="${link}">${link}</a></li>
                @endfor
                </ul>
            </li>
        @else
            <p>No matching document found.</p>
        @endfor
    </ol>
</body>

</html>
