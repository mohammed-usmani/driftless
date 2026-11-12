package dev.driftless.server.search;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.util.Map;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SearchController {

    /** {@code filter} maps a metadata column to a value or a list of accepted values. */
    public record SearchRequest(@NotBlank @Size(max = 2000) String query, @Min(1) @Max(100) Integer k,
                                Map<String, Object> filter, String principal) { }

    private final SearchService search;

    public SearchController(SearchService search) {
        this.search = search;
    }

    @PostMapping("/v1/pipelines/{name}/search")
    public SearchService.Result search(@PathVariable String name, @Valid @RequestBody SearchRequest req) {
        return search.search(name, req.query(), req.k() == null ? 10 : req.k(), req.filter(), req.principal());
    }
}
